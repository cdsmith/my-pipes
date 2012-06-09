{-# LANGUAGE FlexibleInstances  #-}
{-# LANGUAGE GADTs              #-}
{-# LANGUAGE TupleSections      #-}
{-# LANGUAGE TypeFamilies       #-}
{-# LANGUAGE Rank2Types         #-}

module Pipes (
    MonadStream(..),
    MonadUnStream(..),
    Leftovers,
    NoLeftovers,
    Pipe,
    (>+>),
    simulatePipe,
    runPipe,
    PipeC(..),
    FinalC(..),
    await,
    withAwait,
    forP,
    mapP,
    concatMapP,
    filterP,
    idP,
    foldP,
    takeP,
    dropP,
    fromList,
    consume,
    consumeToo,
    peek,
    leftP,
    rightP,
    collectLeftovers,
    discardLeftovers
    ) where

import Prelude hiding (id, (.))
import Control.Applicative
import Control.Arrow (first)
import Control.Category
import Control.Exception
import Control.Monad
import Control.Monad.State
import Control.Monad.Trans
import Control.Monad.Trans.Either
import Data.Monoid
import Data.Typeable
import Data.Void

class Monad m => MonadStream m where
    type Upstream     m
    type Downstream   m
    type StreamResult m

    tryAwait :: m (Either (StreamResult m) (Upstream m))
    yield    :: Downstream m -> m ()

class MonadStream m => MonadUnStream m where
    unawait :: Upstream m -> m ()

data Leftovers
data NoLeftovers

data Pipe lo a b u m r where
    Yield   :: b -> Pipe lo a b u m r -> Pipe lo a b u m r
    Await   :: (Either u a -> Pipe lo a b u m r) -> Pipe lo a b u m r
    UnAwait :: a -> Pipe Leftovers a b u m r -> Pipe Leftovers a b u m r
    Do      :: m (Pipe lo a b u m r) -> Pipe lo a b u m r
    Done    :: r -> Pipe lo a b u m r

instance Monad m => Monad (Pipe lo a b u m) where
    return  = Done

    Yield   x p >>= f = Yield   x (p >>= f)
    Await   g   >>= f = Await   ((>>= f) . g)
    UnAwait x p >>= f = UnAwait x (p >>= f)
    Do      m   >>= f = Do      (liftM (>>= f) m)
    Done    r   >>= f = f r

instance Monad m => Functor (Pipe lo a b u m) where fmap  = liftM
instance Monad m => Applicative (Pipe lo a b u m) where pure = return ; (<*>) = ap
instance MonadTrans (Pipe lo a b u) where lift m = Do (liftM Done m)
instance MonadIO m => MonadIO (Pipe lo a b u m) where liftIO = lift . liftIO

instance Monad m => MonadStream (Pipe lo a b u m) where
    type Upstream     (Pipe lo a b u m) = a
    type Downstream   (Pipe lo a b u m) = b
    type StreamResult (Pipe lo a b u m) = u

    tryAwait = Await (either (Done . Left) (Done . Right))
    yield x  = Yield x (Done ())

instance Monad m => MonadUnStream (Pipe Leftovers a b u m) where
    unawait x = UnAwait x (Done ())

(>+>) :: Monad m => Pipe lo a b r m s -> Pipe NoLeftovers b c s m t -> Pipe lo a c r m t
p >+> Yield x q = Yield x (p >+> q)
p >+> Do    m   = Do    (liftM (p >+>) m)
p >+> Done  x   = Done  x
p >+> Await f   = upstream p
    where upstream (Yield   x q) = q       >+> f (Right x)
          upstream (Done    x  ) = Done x  >+> f (Left  x)
          upstream (Do      m  ) = Do (liftM upstream m)
          upstream (Await   g  ) = Await (upstream . g)
          upstream (UnAwait x q) = UnAwait x (upstream q)

simulatePipe :: (Monad m, MonadTrans t, Monad (t m))
             => t m (Either u a)
             -> (b -> t m ())
             -> Pipe lo a b u m r
             -> t m (r, [a])
simulatePipe up down = go []
    where go xs     (Yield   x p) = down x >> go xs p
          go (x:xs) (Await   f)   = go xs (f (Right x))
          go []     (Await   f)   = go [] . f =<< up
          go xs     (UnAwait x p) = go (x:xs) p
          go xs     (Do      m)   = lift m >>= go xs
          go xs     (Done    x)   = return (x, xs)

newtype IdentityT m a = IdentityT { runIdentityT :: m a }

instance Monad m => Monad (IdentityT m) where
    return            = IdentityT . return
    IdentityT m >>= f = IdentityT $ m >>= runIdentityT . f

instance MonadTrans IdentityT where lift = IdentityT
instance MonadIO m => MonadIO (IdentityT m) where liftIO = lift . liftIO

runPipe :: Monad m => Pipe lo () Void u m r -> m r
runPipe = liftM fst
        . runIdentityT
        . simulatePipe (return (Right ()))
                       (error "runPipe: impossible yield")

newtype PipeC m r a b = PipeC (Pipe NoLeftovers a b r m r)
instance Monad m => Category (PipeC m r) where
    id                    = PipeC idP
    (PipeC p) . (PipeC q) = PipeC (q >+> p)

newtype FinalC a m u r = FinalC (Pipe NoLeftovers a a u m r)
instance Monad m => Category (FinalC a m) where
    id                      = FinalC idP
    (FinalC p) . (FinalC q) = FinalC (q >+> p)

instance MonadStream m => MonadStream (EitherT e m) where
    type Upstream     (EitherT e m) = Upstream     m
    type Downstream   (EitherT e m) = Downstream   m
    type StreamResult (EitherT e m) = StreamResult m

    yield = lift . yield
    tryAwait = lift tryAwait

instance MonadUnStream m => MonadUnStream (EitherT e m) where
    unawait = lift . unawait

await :: MonadStream m => EitherT (StreamResult m) m (Upstream m)
await = tryAwait >>= either left return

withAwait :: MonadStream m => EitherT (StreamResult m) m (StreamResult m) -> m (StreamResult m)
withAwait = liftM (either id id) . runEitherT

forP :: MonadStream m => (Upstream m -> m r) -> m (StreamResult m)
forP f = tryAwait >>= either return ((>> forP f) . f)

mapP :: MonadStream m => (Upstream m -> Downstream m) -> m (StreamResult m)
mapP f = forP (yield . f)

concatMapP :: MonadStream m => (Upstream m -> [Downstream m]) -> m (StreamResult m)
concatMapP f = forP (mapM_ yield . f)

filterP :: (MonadStream m, Upstream m ~ Downstream m) => (Upstream m -> Bool) -> m (StreamResult m)
filterP f = forP $ \x -> when (f x) (yield x)

idP :: (MonadStream m, Upstream m ~ Downstream m) => m (StreamResult m)
idP = mapP id

foldP :: MonadStream m => (a -> Upstream m -> a) -> a -> m (a, StreamResult m)
foldP f x = tryAwait >>= either (return . (x,)) (foldP f . f x)

takeP :: (MonadStream m, Upstream m ~ Downstream m) => Int -> m ()
takeP 0 = return ()
takeP n = tryAwait >>= either (const (return ())) ((>> takeP (n-1)) . yield)

dropP :: (MonadStream m, Upstream m ~ Downstream m) => Int -> m (StreamResult m)
dropP 0 = idP
dropP n = tryAwait >>= either return (const (dropP (n-1)))

fromList :: MonadStream m => [Downstream m] -> m ()
fromList xs = mapM_ yield xs

consume :: (MonadStream m, Downstream m ~ Void) => m [Upstream m]
consume = tryAwait >>= either (const $ return []) (\x -> liftM (x:) consume)

consumeToo :: (MonadStream m, Downstream m ~ Void) => m ([Upstream m], StreamResult m)
consumeToo = tryAwait >>= either (return . ([],)) (\x -> liftM (first (x:)) consumeToo)

peek :: MonadUnStream m => m (Either (StreamResult m) (Upstream m))
peek = tryAwait >>= either (return . Left) (\x -> unawait x >> return (Right x))

leftP :: Monad m => Pipe lo a b u m r -> Pipe lo (Either a c) (Either b c) u m r
leftP p = undefined

rightP :: Monad m => Pipe lo a b r m s -> Pipe lo (Either c a) (Either c b) r m s
rightP p = undefined

collectLeftovers :: MonadIO m => Pipe Leftovers a b u m r -> Pipe NoLeftovers a b u m (r, [a])
collectLeftovers = undefined

discardLeftovers :: MonadIO m => Pipe Leftovers a b u m r -> Pipe NoLeftovers a b u m r
discardLeftovers = undefined
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies  #-}
{-# LANGUAGE Rank2Types    #-}

module Pipes (
    MonadStream,
    tryAwait,
    yield,
    Pipe,
    (>+>),
    simulatePipe,
    runPipe,
    PipeC(..),
    FinalC(..),
    forP,
    mapP,
    concatMapP,
    filterP,
    idP,
    consume,
    consumeToo,
    leftP,
    rightP,
    PutbackPipe,
    putback,
    toPutback,
    runPutback,
    discardPutback,
    (>++>)
    ) where

import Prelude hiding (id, (.))
import Control.Arrow (first)
import Control.Applicative
import Control.Monad
import Control.Monad.Trans
import Control.Monad.State
import Control.Category
import Data.Monoid
import Data.Void

class Monad m => MonadStream m where
    type Upstream     m
    type Downstream   m
    type StreamResult m

    tryAwait :: m (Either (StreamResult m) (Upstream m))
    yield    :: Downstream m -> m ()

data Pipe a b r m s = Yield b (Pipe a b r m s)
                    | Await (Either r a -> Pipe a b r m s)
                    | Do    (m (Pipe a b r m s))
                    | Done  s

instance Monad m => Monad (Pipe a b r m) where
    return  = Done

    Yield x p >>= f = Yield x (p >>= f)
    Await g   >>= f = Await ((>>= f) . g)
    Do    m   >>= f = Do    (liftM (>>= f) m)
    Done  r   >>= f = f r

instance Monad m => Functor (Pipe a b r m) where fmap  = liftM
instance Monad m => Applicative (Pipe a b r m) where pure = return ; (<*>) = ap
instance MonadTrans (Pipe a b r) where lift m = Do (liftM Done m)
instance MonadIO m => MonadIO (Pipe a b r m) where liftIO = lift . liftIO

instance Monad m => MonadStream (Pipe a b r m) where
    type Upstream     (Pipe a b r m) = a
    type Downstream   (Pipe a b r m) = b
    type StreamResult (Pipe a b r m) = r

    tryAwait = Await Done
    yield x  = Yield x (Done ())

(>+>) :: Monad m => Pipe a b r m s -> Pipe b c s m t -> Pipe a c r m t
p >+> Yield x q = Yield x (p >+> q)
p >+> Do    m   = Do    (liftM (p >+>) m)
p >+> Done  x   = Done  x
p >+> Await f   = upstream p
    where upstream (Yield x q) = q      >+> f (Right x)
          upstream (Done  x  ) = Done x >+> f (Left  x)
          upstream (Do    m  ) = Do    (liftM upstream m)
          upstream (Await g  ) = Await (upstream . g)

simulatePipe :: (Monad m, MonadTrans t, Monad (t m))
             => t m (Either r a)
             -> (b -> t m ())
             -> Pipe a b r m s
             -> t m s
simulatePipe up down (Yield x p) = down x >> simulatePipe up down p
simulatePipe up down (Await f)   = simulatePipe up down . f =<< up
simulatePipe up down (Do    m)   = lift m >>= simulatePipe up down
simulatePipe up down (Done  x)   = return x

newtype IdentityT m a = IdentityT { runIdentityT :: m a }

instance Monad m => Monad (IdentityT m) where
    return            = IdentityT . return
    IdentityT m >>= f = IdentityT $ m >>= runIdentityT . f

instance MonadTrans IdentityT where lift = IdentityT

runPipe :: Monad m => Pipe () Void r m s -> m s
runPipe = runIdentityT . simulatePipe (return (Right ()))
                                      (error "runPipe: impossible yield of Void")

newtype PipeC m r a b = PipeC (Pipe a b r m r)
instance Monad m => Category (PipeC m r) where
    id                    = PipeC idP
    (PipeC p) . (PipeC q) = PipeC (q >+> p)

newtype FinalC a m r s = FinalC (Pipe a a r m s)
instance Monad m => Category (FinalC a m) where
    id                      = FinalC idP
    (FinalC p) . (FinalC q) = FinalC (q >+> p)

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

leftP :: Monad m => Pipe a b r m s -> Pipe (Either a c) (Either b c) r m s
leftP p = simulatePipe up down p
    where up   = do x <- tryAwait
                    case x of Left  r         -> return (Left r)
                              Right (Left  x) -> return (Right x)
                              Right (Right x) -> yield (Right x) >> up
          down = yield . Left

rightP :: Monad m => Pipe a b r m s -> Pipe (Either c a) (Either c b) r m s
rightP p = simulatePipe up down p
    where up   = do x <- tryAwait
                    case x of Left  r         -> return (Left r)
                              Right (Right x) -> return (Right x)
                              Right (Left  x) -> yield (Left x) >> up
          down = yield . Right

newtype PutbackPipe a b r m s = PutbackPipe { unPutback :: Pipe a (Either a b) r m s }

instance Monad m => Monad (PutbackPipe a b r m) where
    return  = PutbackPipe . return
    PutbackPipe p >>= f = PutbackPipe (p >>= unPutback . f)

instance Monad m => Functor (PutbackPipe a b r m) where fmap  = liftM
instance Monad m => Applicative (PutbackPipe a b r m) where pure = return ; (<*>) = ap
instance MonadTrans (PutbackPipe a b r) where lift m = PutbackPipe (Do (liftM Done m))
instance MonadIO m => MonadIO (PutbackPipe a b r m) where liftIO = lift . liftIO

instance Monad m => MonadStream (PutbackPipe a b r m) where
    type Upstream     (PutbackPipe a b r m) = a
    type Downstream   (PutbackPipe a b r m) = b
    type StreamResult (PutbackPipe a b r m) = r

    tryAwait = PutbackPipe tryAwait
    yield    = PutbackPipe . yield . Right

putback :: Monad m => a -> PutbackPipe a b r m ()
putback = PutbackPipe . yield . Left

toPutback :: Monad m => Pipe a b r m s -> PutbackPipe a b r m s
toPutback p = PutbackPipe (p >+> mapP Right)

newtype PBWrapper a b r m s = PBWrap { unwrapPB :: StateT [a] (Pipe a b r m) s }

instance Monad m => Monad (PBWrapper a b r m) where
    return = PBWrap . return
    (PBWrap m) >>= f = PBWrap (m >>= unwrapPB . f)

instance MonadTrans (PBWrapper a b r) where lift = PBWrap . lift . lift

runPutback :: Monad m => PutbackPipe a b r m s -> Pipe a b r m (s, [a])
runPutback (PutbackPipe p) = runStateT (unwrapPB (simulatePipe up down p)) []
    where up = PBWrap $ do lo <- get
                           case lo of []   -> lift tryAwait
                                      x:xs -> put xs >> return (Right x)
          down (Left  x) = PBWrap $ modify (x:)
          down (Right x) = PBWrap $ lift (yield x)

discardPutback :: Monad m => PutbackPipe a b r m s -> Pipe a b r m s
discardPutback p = liftM fst (runPutback p)

(>++>) :: Monad m => PutbackPipe a b r m s -> Pipe b c s m t -> PutbackPipe a c r m t
PutbackPipe p >++> q = PutbackPipe (p >+> rightP q)

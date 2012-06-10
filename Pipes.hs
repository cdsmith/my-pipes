{-# LANGUAGE FlexibleInstances  #-}
{-# LANGUAGE FlexibleContexts   #-}
{-# LANGUAGE GADTs              #-}
{-# LANGUAGE TupleSections      #-}
{-# LANGUAGE TypeFamilies       #-}
{-# LANGUAGE Rank2Types         #-}

-- | This module provides support for streaming I/O in Haskell.  It provides
-- the ability for monads to 'await' input from upstream stages, and 'yield'
-- output to downstream stages, at any step of a computation.
--
-- The operations are captured by the 'MonadStream' type class.  A subclass
-- 'MonadUnStream' permits the computation to also yield leftover values back
-- upstream when it doesn't need to consume them all yet.  The standard
-- implementation is 'Pipe'.

module Pipes (
    -- * Classes
    MonadStream(..),
    MonadUnStream(..),

    -- * The 'Pipe' type
    Pipe,
    Leftovers,
    NoLeftovers,

    -- * Composition
    (>+>),
    (<+<),

    -- * 'Either' plumbing combinators
    leftP,
    rightP,
    leftResultP,
    rightResultP,

    -- * Leftover handling
    collectLeftovers,
    discardLeftovers,

    -- * Running pipes
    simulatePipe,
    runPipe,

    -- * The 'await' operation
    await,
    withAwait,

    -- * Miscellaneous combinators
    mapResultP,
    forP,
    mapP,
    concatMapP,
    filterP,
    idP,
    foldP,
    takeP,
    dropP,
    fromList,
    mconcatP,
    consume,
    consumeToo,
    peek,

    -- * 'Category' instances
    PipeC(..),
    FinalC(..)
    ) where

import Prelude hiding (id, (.))
import Control.Applicative
import Control.Arrow hiding (left)
import Control.Category
import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Cont
import Control.Monad.Trans.Either
import Control.Monad.Trans.Error
import Control.Monad.Trans.List
import Control.Monad.Trans.Identity
import Control.Monad.Trans.Maybe
import Control.Monad.Trans.Reader
import Control.Monad.Trans.State
import Control.Monad.Trans.Writer
import Control.Monad.Trans.RWS
import Data.Monoid
import Data.Void

-- | Class for monads that can await upstream and yield downstream values.  The
-- core implementation for this class is 'Pipe', but the class allows pipes to
-- be easily wrapped in monad transformers.
class Monad m => MonadStream m where
    -- | The type of values to expect yielded from upstream.
    type Upstream     m
    -- | The type of values to yield to downstream.
    type Downstream   m
    -- | The type of the final result when upstream terminates.
    type StreamResult m

    -- | Waits for a value from upstream.  If a value is produced, returns
    -- it.  If the upstream stage terminates without producing a value, gives
    -- the return value from upstream instead.
    --
    -- If the intent is that upstream termination should immediately finish
    -- this stage as well with the same return value, consider using 'await'
    -- and its partner 'withAwait' instead.
    tryAwait :: m (Either (StreamResult m) (Upstream m))

    -- | Yields a value to downstream.  This operation may never return if
    -- the downstream stage never awaits another value.
    yield    :: Downstream m -> m ()

instance MonadStream m => MonadStream (IdentityT m) where
    type Upstream     (IdentityT m) = Upstream     m
    type Downstream   (IdentityT m) = Downstream   m
    type StreamResult (IdentityT m) = StreamResult m

    yield = lift . yield
    tryAwait = lift tryAwait

instance MonadStream m => MonadStream (ListT m) where
    type Upstream     (ListT m) = Upstream     m
    type Downstream   (ListT m) = Downstream   m
    type StreamResult (ListT m) = StreamResult m

    yield = lift . yield
    tryAwait = lift tryAwait

instance MonadStream m => MonadStream (MaybeT m) where
    type Upstream     (MaybeT m) = Upstream     m
    type Downstream   (MaybeT m) = Downstream   m
    type StreamResult (MaybeT m) = StreamResult m

    yield = lift . yield
    tryAwait = lift tryAwait

instance MonadStream m => MonadStream (EitherT e m) where
    type Upstream     (EitherT e m) = Upstream     m
    type Downstream   (EitherT e m) = Downstream   m
    type StreamResult (EitherT e m) = StreamResult m

    yield = lift . yield
    tryAwait = lift tryAwait

instance MonadStream m => MonadStream (ContT r m) where
    type Upstream     (ContT r m) = Upstream     m
    type Downstream   (ContT r m) = Downstream   m
    type StreamResult (ContT r m) = StreamResult m

    yield = lift . yield
    tryAwait = lift tryAwait

instance (MonadStream m, Error e) => MonadStream (ErrorT e m) where
    type Upstream     (ErrorT e m) = Upstream     m
    type Downstream   (ErrorT e m) = Downstream   m
    type StreamResult (ErrorT e m) = StreamResult m

    yield = lift . yield
    tryAwait = lift tryAwait

instance MonadStream m => MonadStream (ReaderT r m) where
    type Upstream     (ReaderT r m) = Upstream     m
    type Downstream   (ReaderT r m) = Downstream   m
    type StreamResult (ReaderT r m) = StreamResult m

    yield = lift . yield
    tryAwait = lift tryAwait

instance MonadStream m => MonadStream (StateT s m) where
    type Upstream     (StateT s m) = Upstream     m
    type Downstream   (StateT s m) = Downstream   m
    type StreamResult (StateT s m) = StreamResult m

    yield = lift . yield
    tryAwait = lift tryAwait

instance (MonadStream m, Monoid w) => MonadStream (WriterT w m) where
    type Upstream     (WriterT w m) = Upstream     m
    type Downstream   (WriterT w m) = Downstream   m
    type StreamResult (WriterT w m) = StreamResult m

    yield = lift . yield
    tryAwait = lift tryAwait

instance (MonadStream m, Monoid w) => MonadStream (RWST r w s m) where
    type Upstream     (RWST r w s m) = Upstream     m
    type Downstream   (RWST r w s m) = Downstream   m
    type StreamResult (RWST r w s m) = StreamResult m

    yield = lift . yield
    tryAwait = lift tryAwait

-- | Class for monads that, in addition to the operations from 'MonadStream',
-- can also give back unused values that were awaited from upstream.  The main
-- implementation is still 'Pipe', but this instance only exists for pipes
-- built with 'Leftovers'.
class MonadStream m => MonadUnStream m where
    -- | Gives back a value, which will be returned from the next use of
    -- 'tryAwait' or 'await'.  This is normally used to put back values that
    -- are not required.  For example, when reading a stream of 'ByteString'
    -- for parsing, a single 'ByteString' may contain too much data, and the
    -- extra data should be made available for the next await.
    --
    -- Leftovers are obtained back in the opposite order in which they are
    -- 'unawait'ed, treating the upstream as a kind of stack.
    unawait :: Upstream m -> m ()

instance (MonadUnStream m          ) => MonadUnStream (IdentityT m)  where unawait = lift . unawait
instance (MonadUnStream m          ) => MonadUnStream (ListT m)      where unawait = lift . unawait
instance (MonadUnStream m          ) => MonadUnStream (MaybeT m)     where unawait = lift . unawait
instance (MonadUnStream m          ) => MonadUnStream (EitherT e m)  where unawait = lift . unawait
instance (MonadUnStream m          ) => MonadUnStream (ContT r m)    where unawait = lift . unawait
instance (MonadUnStream m, Error e ) => MonadUnStream (ErrorT e m)   where unawait = lift . unawait
instance (MonadUnStream m          ) => MonadUnStream (ReaderT r m)  where unawait = lift . unawait
instance (MonadUnStream m          ) => MonadUnStream (StateT s m)   where unawait = lift . unawait
instance (MonadUnStream m, Monoid w) => MonadUnStream (WriterT w m)  where unawait = lift . unawait
instance (MonadUnStream m, Monoid w) => MonadUnStream (RWST r w s m) where unawait = lift . unawait

-- | The central type for streaming monadic actions.  A 'Pipe' wraps another
-- monad to add the capabilities of 'MonadStream', and optionally of
-- 'MonadUnStream' as well.  The type parameters are:
--
-- * @lo@ - Should be either 'Leftovers' or 'NoLeftovers', as appropriate.
-- * @a@  - The upstream data type that this 'Pipe' expects when it awaits.
-- * @b@  - The downstream data type that this 'Pipe' yields to others.
-- * @u@  - The upstream result, obtained when the upstream 'Pipe' ends.
-- * @m@  - The base monad wrapped by the 'Pipe'
-- * @r@  - The result type produced when this 'Pipe' ends.
data Pipe lo a b u m r where
    Yield   :: b -> Pipe lo a b u m r -> Pipe lo a b u m r
    Await   :: (Either u a -> Pipe lo a b u m r) -> Pipe lo a b u m r
    UnAwait :: a -> Pipe Leftovers a b u m r -> Pipe Leftovers a b u m r
    Do      :: m (Pipe lo a b u m r) -> Pipe lo a b u m r
    Done    :: r -> Pipe lo a b u m r

-- | Phantom type to indicate that a 'Pipe' can yield leftovers, and is an
-- instance of the 'MonadUnStream' class.
data Leftovers

-- | Phantom type to indicate that a 'Pipe' cannot yield leftovers, and is
-- not an instance of the 'MonadUnStream' class.
data NoLeftovers

instance Monad m => Monad (Pipe lo a b u m) where
    return  = Done

    Yield   x p >>= f = Yield   x (p >>= f)
    Await   g   >>= f = Await   ((>>= f) . g)
    UnAwait x p >>= f = UnAwait x (p >>= f)
    Do      m   >>= f = Do      (liftM (>>= f) m)
    Done    r   >>= f = f r

instance Monad m => Functor (Pipe lo a b u m) where fmap  = liftM
instance Monad m => Applicative (Pipe lo a b u m) where pure = return; (<*>) = ap
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

-- | The 'Pipe' composition operator.  Combines two pipes, matching the yields
-- and returns from the upstream pipe to the awaits of the downstream pipe.
-- Composition is associative, and 'idP' acts as an identity, so that '(>+>)'
-- is the composition for a category on pipes without leftovers.
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

-- | Like '(>+>)', except in the opposite order.
(<+<) :: Monad m => Pipe NoLeftovers b c s m t -> Pipe lo a b r m s -> Pipe lo a c r m t
(<+<) = flip (>+>)

-- | Modifies a 'Pipe' to act on the left side of an 'Either' type.  The right
-- side is passed through unchanged.
leftP :: Monad m => Pipe lo a b u m r -> Pipe lo (Either a c) (Either b c) u m r
leftP (Yield   x p) = Yield   (Left x) (leftP p)
leftP (UnAwait x p) = UnAwait (Left x) (leftP p)
leftP (Do      m  ) = Do      (liftM leftP m)
leftP (Done    r  ) = Done    r
leftP (Await   f  ) = Await   go
    where go (Left  r        ) = leftP (f (Left r))
          go (Right (Left  a)) = leftP (f (Right a))
          go (Right (Right c)) = Yield (Right c) (Await go)

-- | Modifies a 'Pipe' to act on the right side of an 'Either' type.  The left
-- side is passed through unchanged.
rightP :: Monad m => Pipe lo a b u m r -> Pipe lo (Either c a) (Either c b) u m r
rightP (Yield   x p) = Yield   (Right x) (rightP p)
rightP (UnAwait x p) = UnAwait (Right x) (rightP p)
rightP (Do      m  ) = Do      (liftM rightP m)
rightP (Done    r  ) = Done    r
rightP (Await   f  ) = Await   go
    where go (Left  r        ) = rightP (f (Left r))
          go (Right (Right a)) = rightP (f (Right a))
          go (Right (Left  c)) = Yield (Left c) (Await go)

-- | Modifies a 'Pipe' to act on the left side of an 'Either' return type.  If
-- the upstream pipe terminates with a 'Right' value, the new pipe also
-- terminates with the same value.
leftResultP :: Monad m => Pipe lo a b u m r -> Pipe lo a b (Either u s) m (Either r s)
leftResultP (Yield   x p) = Yield   x (leftResultP p)
leftResultP (UnAwait x p) = UnAwait x (leftResultP p)
leftResultP (Do      m  ) = Do      (liftM leftResultP m)
leftResultP (Done    r  ) = Done    (Left r)
leftResultP (Await   f  ) = Await   go
    where go (Right a        ) = leftResultP (f (Right a))
          go (Left  (Left  u)) = leftResultP (f (Left  u))
          go (Left  (Right s)) = Done  (Right s)

-- | Modified a 'Pipe' to act on the right side of an 'Either' return type.  If
-- the upstream pipe terminates with a 'Left' value, the new pipe also
-- terminates with the same value.
rightResultP :: Monad m => Pipe lo a b u m r -> Pipe lo a b (Either s u) m (Either s r)
rightResultP (Yield   x p) = Yield   x (rightResultP p)
rightResultP (UnAwait x p) = UnAwait x (rightResultP p)
rightResultP (Do      m  ) = Do      (liftM rightResultP m)
rightResultP (Done    r  ) = Done    (Right r)
rightResultP (Await   f  ) = Await   go
    where go (Right a        ) = rightResultP (f (Right a))
          go (Left  (Right u)) = rightResultP (f (Left  u))
          go (Left  (Left  s)) = Done   (Left s)

-- | Converts a 'Pipe' with 'Leftovers' into a 'Pipe' without 'Leftovers',
-- which just returns its leftover values as part of the result.  The leftovers
-- returned are in the order in which they would be obtained from future
-- awaits, so the first element of the list would be the first obtained (and
-- was therefore the last value unawaited).
collectLeftovers :: Monad m => Pipe Leftovers a b u m r -> Pipe NoLeftovers a b u m (r, [a])
collectLeftovers = go []
    where go xs     (Yield   x p) = Yield x (go xs p)
          go xs     (UnAwait x p) = go (x:xs) p
          go (x:xs) (Await   f  ) = go xs (f (Right x))
          go []     (Await   f  ) = Await (go [] . f)
          go xs     (Do      m  ) = Do    (liftM (go xs) m)
          go xs     (Done    r  ) = Done  (r, xs)

-- | Converts a 'Pipe' with 'Leftovers' into a 'Pipe' without 'Leftovers',
-- by discarding any leftovers remaining after the original pipe terminates.
discardLeftovers :: Monad m => Pipe Leftovers a b u m r -> Pipe NoLeftovers a b u m r
discardLeftovers = fmap fst . collectLeftovers

-- | Executes a pipe in a simulated environment.  The environment is
-- represented by a monad transformer, and actions are provided to respond to
-- the awaits and yields.  This can be used to execute single stages of a pipe
-- that are not yet a complete pipeline.  Note that since 'Pipe' is itself a
-- monad transformer, this can also be used to isolate a pipe and run it with
-- checks in the context of a surrounding pipeline.
--
-- To simulate a pipe with leftovers, first use 'collectLeftovers' or
-- 'discardLeftovers' to specify what to do with them.
simulatePipe :: (Monad m, MonadTrans t, Monad (t m))
             => t m (Either u a)
             -> (b -> t m ())
             -> Pipe NoLeftovers a b u m r
             -> t m r
simulatePipe up down (Yield   x p) = down x >> simulatePipe up down p
simulatePipe up down (Await   f)   = simulatePipe up down . f =<< up
simulatePipe up down (Do      m)   = lift m >>= simulatePipe up down
simulatePipe up down (Done    x)   = return x

-- | Executes a complete pipeline, giving back the result.  The upstream end is
-- fed an infinite stream of unit values, and the downstream end is set to Void
-- so that no yields are possible (except for bottoms, which will result in
-- runtime errors).
--
-- To run a pipe with leftovers, first use 'collectLeftovers' or
-- 'discardLeftovers' to specify what to do with them.
runPipe :: Monad m => Pipe NoLeftovers () Void u m r -> m r
runPipe = runIdentityT
        . simulatePipe (return (Right ()))
                       (error "runPipe: impossible yield")

-- | A version of 'tryAwait' that directly returns the upstream value.  If the
-- upstream pipe terminates instead, the result will be a 'Left' value, which
-- automatically propogates in the 'EitherT' monad.  Uses of 'await' are
-- usually paired with a corresponding 'withAwait' that is used to produce the
-- upstream result as a return value.
--
-- For example:
-- > p = withAwait $ do
-- >         ...
-- >         x <- await
-- >         ...
await :: MonadStream m => EitherT (StreamResult m) m (Upstream m)
await = tryAwait >>= either left return

-- | A wrapper for results of 'await', which takes upstream termination and
-- turns it into the result of the current pipe.  Usually, you will use
-- 'withAwait' at the top level immediately before composing the pipe with
-- another.
withAwait :: MonadStream m => EitherT (StreamResult m) m (StreamResult m) -> m (StreamResult m)
withAwait = liftM (either id id) . runEitherT

-- | Lifts a function into an identity pipe that transforms the result.  In
-- general, @p >+> mapResultP f == liftM f p@, but it's occasionally convenient
-- to use composition instead of 'liftM' or 'fmap'.  This embeds the category
-- of functions inside the 'FinalC' category in a way that preserves
-- composition and identities.
mapResultP :: (MonadStream m, Upstream m ~ Downstream m) => (StreamResult m -> r) -> m r
mapResultP f = liftM f idP

-- | Performs a given stream action for each upstream value, finishing by
-- keeping the upstream return value.  This can be used to perform many kinds
-- of elementwise processing on a stream of values.
forP :: MonadStream m => (Upstream m -> m r) -> m (StreamResult m)
forP f = tryAwait >>= either return ((>> forP f) . f)

-- | Lifts a function to a pipe that applies the function to each upstream
-- value, keeping the upstream return value.  This embeds the category of
-- functions inside the 'PipeC' category in a way that preserves composition
-- and identities.
mapP :: MonadStream m => (Upstream m -> Downstream m) -> m (StreamResult m)
mapP f = forP (yield . f)

-- | Lifts a function to a pipe, where the function maps each single input
-- to many output values.
concatMapP :: MonadStream m => (Upstream m -> [Downstream m]) -> m (StreamResult m)
concatMapP f = forP (mapM_ yield . f)

-- | A pipe that passes through elements matching a predicate, and discards all
-- others.
filterP :: (MonadStream m, Upstream m ~ Downstream m) => (Upstream m -> Bool) -> m (StreamResult m)
filterP f = forP $ \x -> when (f x) (yield x)

-- | The identity pipe.  This is the identity for both the 'PipeC' and 'FinalC'
-- categories.  It passes through all upstream values, and then returns with
-- the upstream return.
idP :: (MonadStream m, Upstream m ~ Downstream m) => m (StreamResult m)
idP = mapP id

-- | A pipe that accumulates its input, and then returns the result, in a
-- manner like a left fold.  The pipe never yields values.
foldP :: MonadStream m => (a -> Upstream m -> a) -> a -> m (a, StreamResult m)
foldP f x = tryAwait >>= either (return . (x,)) (foldP f . f x)

-- | A pipe that passes through the first @n@ values, for some @n@, and then
-- terminates.  If the upstream pipe terminates before that, then this one
-- does, too.
takeP :: (MonadStream m, Upstream m ~ Downstream m) => Int -> m ()
takeP 0 = return ()
takeP n = tryAwait >>= either (const (return ())) ((>> takeP (n-1)) . yield)

-- | A pipe that drops the first @n@ values, and then acts like the identity.
-- If the upstream pipe yields fewer than @n@ values, then this pipe drops
-- them all and terminates, too.
dropP :: (MonadStream m, Upstream m ~ Downstream m) => Int -> m (StreamResult m)
dropP 0 = idP
dropP n = tryAwait >>= either return (const (dropP (n-1)))

-- | Converts a list to a pipe that yields each element in turn, and then
-- terminates.  If the list is infinite, then the pipe yields elements forever.
fromList :: MonadStream m => [Downstream m] -> m ()
fromList xs = mapM_ yield xs

-- | Concatenates a stream of values of some monoid.  The pipe yields nothing,
-- but terminates when the upstream does, with the resulting concatenated
-- value.
mconcatP :: (MonadStream m, Monoid (Upstream m)) => m (Upstream m)
mconcatP = liftM fst (foldP mappend mempty)

-- | A pipe that collects all values yielded by upstream, and returns them in
-- a list.
consume :: (MonadStream m, Downstream m ~ Void) => m [Upstream m]
consume = tryAwait >>= either (const $ return []) (\x -> liftM (x:) consume)

-- | A pipe that collects all values yielded by upstream, and returns them in
-- a list, along with the upstream return value.
consumeToo :: (MonadStream m, Downstream m ~ Void) => m ([Upstream m], StreamResult m)
consumeToo = tryAwait >>= either (return . ([],)) (\x -> liftM (first (x:)) consumeToo)

-- | A pipe that returns, but does not consume, an upstream value.
peek :: MonadUnStream m => m (Either (StreamResult m) (Upstream m))
peek = tryAwait >>= either (return . Left) (\x -> unawait x >> return (Right x))

-- | This category wraps 'Pipe' as a promise that pipe composition forms a
-- category for pipes without leftovers, with respect to the upstream and
-- downstream data types.
newtype PipeC m r a b = PipeC (Pipe NoLeftovers a b r m r)
instance Monad m => Category (PipeC m r) where
    id                    = PipeC idP
    (PipeC p) . (PipeC q) = PipeC (q >+> p)

-- | This category wraps 'Pipe' as a promise that pipe composition forms a
-- category for pipes without leftovers, with respect to the upstream and
-- downstream result types.
newtype FinalC a m u r = FinalC (Pipe NoLeftovers a a u m r)
instance Monad m => Category (FinalC a m) where
    id                      = FinalC idP
    (FinalC p) . (FinalC q) = FinalC (q >+> p)

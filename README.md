my-pipes
========

Alternative implementation of the pipes concept.

The Pipe type is `Pipe lo a b u m r`, where:

  - `lo` is either `Leftovers` or `NoLeftovers`
  - `a` is the input type
  - `b` is the output type
  - `u` is the upstream return type
  - `m` is the base monad
  - `r` is the return type

The primitive stream operations are:

  - `tryAwait` waits for upstream `yield` or termination
  - `yield` yields a value to downstream

The `await` operation is provided via `EitherT`, and a corresponding `withAwait` to
unwrap the `EitherT` at the top level.

Exceptions and finalization are to be decided.

Note that `simulatePipe` is the fundamental abstraction for running a `Pipe`.  The more
widely known `runPipe` is provided as a specialization.

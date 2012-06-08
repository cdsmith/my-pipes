my-pipes
========

Alternative implementation of the pipes concept.

The Pipe type is `Pipe a b u m r`, where:

  - `a` is the input type
  - `b` is the output type
  - `u` is the upstream return type
  - `m` is the base monad
  - `r` is the return type

The primitive stream operations are:

  - `tryAwait` waits for upstream `yield` or termination
  - `yield` yields a value to downstream

Exceptions and finalization are to be decided.

There is a separate `PutbackPipe` type for the purpose of preserving leftover data.  It does
not form a category, and instead provides a specialized composition operator, `>++>`, which
only permits composition on the left end of a pipeline.

Note that `simulatePipe` is the fundamental abstraction for running a `Pipe`.  The more
widely known `runPipe` is provided as a specialization.
//
//  Executor.Runtime.swift
//  swift-executors
//
//  TX-N1D — portable execution-progress interpretation.
//
//  `Executor.Runtime` is additive vocabulary the platform leaves in this
//  package (`Kernel.Thread.Executor`, `.Sharded`, `.Stealing`) already
//  embody informally through their own Teardown Contract documentation.
//  This type gives that state a single, portable name that a leaf can
//  report itself in terms of via a `runtime` accessor -- it does not
//  change any leaf's documented lifetime/teardown contract, and it holds
//  no platform code of its own.
//

extension Executor {
    /// A portable interpretation of an executor's execution-progress state.
    ///
    /// Every executor leaf in this package moves through the same three
    /// states, described precisely by each leaf's own Teardown Contract:
    ///
    /// 1. `.running` -- accepting and executing jobs normally.
    /// 2. `.draining` -- `shutdown()` has been signalled; jobs already
    ///    queued (and, per the Teardown Contract, jobs enqueued during the
    ///    drain window) still execute on the executor's own thread(s).
    /// 3. `.terminated` -- the run loop has verifiably exited. Further
    ///    enqueues run inline on the calling thread rather than hanging.
    ///
    /// `Runtime` only names this state; it never changes how or when a
    /// leaf transitions between states.
    public enum Runtime: Sendable, Equatable {
        /// Accepting and executing jobs normally.
        case running

        /// `shutdown()` has been signalled; the executor is draining its
        /// queue per its Teardown Contract but has not yet exited its
        /// run loop.
        case draining

        /// The run loop has verifiably exited. Enqueue now runs jobs
        /// inline per the Teardown Contract.
        case terminated
    }
}

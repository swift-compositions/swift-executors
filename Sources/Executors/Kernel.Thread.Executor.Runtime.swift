//
//  Kernel.Thread.Executor.Runtime.swift
//  swift-executors
//
//  TX-N1D — additive `Executor.Runtime` accessor for the single-thread
//  leaf. Reads existing state under the executor's own lock; does not
//  add, remove, or reorder any state transition in the Teardown
//  Contract documented on `Kernel.Thread.Executor`.
//

extension Kernel.Thread.Executor {
    /// This executor's current progress state.
    ///
    /// See `Executor.Runtime` and the Teardown Contract documented on
    /// `Kernel.Thread.Executor` for the precise meaning of each case.
    /// This is a read of existing state, taken under the same lock the
    /// run loop and `enqueue` already use -- it does not participate in
    /// or alter the teardown protocol.
    public var runtime: Executor.Runtime {
        wait.withLock {
            if _loopExited { return .terminated }
            if _shutdown.isSet { return .draining }
            return .running
        }
    }
}

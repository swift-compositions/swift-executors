#if canImport(Dispatch)
    extension Executor.Timeout.Ending {
        /// The waiter, the end once it has come and nobody waits for it yet, and whether the wait has ended.
        struct State {
            var waiting: CheckedContinuation<Answer?, Never>?
            var ended: Parcel?
            var done = false
        }
    }
#endif

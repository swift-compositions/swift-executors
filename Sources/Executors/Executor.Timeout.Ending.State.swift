#if canImport(Dispatch)
    extension Executor.Timeout.Ending {
        /// The waiter, and the end once it has come.
        struct State {
            var waiting: CheckedContinuation<Answer?, Never>?
            var ended: Answer??
        }
    }
#endif

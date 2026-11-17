#if canImport(Dispatch)
    extension Executor.Timeout.Ending {
        /// An end on its way to the waiter. It is sendable unchecked because an answer is handed over once:
        /// `end(with:)` takes it as `sending`, at most one end is kept, and that one is given to exactly one waiter, so
        /// no two contexts ever hold it.
        struct Parcel: @unchecked Sendable {
            let answer: Answer?
        }
    }
#endif

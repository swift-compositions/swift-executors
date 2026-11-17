#if canImport(Dispatch)
    extension Executor {
        /// Waits with a time limit on the wall clock.
        ///
        /// A limit here is measured on the wall clock and kept by a timer on a dispatch queue of its own, so it ends
        /// a wait however busy Swift's cooperative pool is, and in a process whose uptime does not move, where a
        /// sleep on `ContinuousClock` or `SuspendingClock` would never end.
        public enum Timeout {}
    }

    extension Executor.Timeout {
        /// What `operation` gives, or nil at the first of: `limit` passing on the wall clock, or the cancellation of
        /// the waiting task.
        ///
        /// The operation runs as a task of its own, isolated as it is declared and preferring `executor` when one is
        /// given, so a call that blocks its thread blocks that one. It is not stopped when the wait ends without it:
        /// what it gives later is dropped. `expired` is called once when the limit ends the wait, never when the
        /// answer or a cancellation does. A limit of no time or less ends the wait at once: `expired` is called and
        /// the operation never starts.
        public static func answer<Answer: Sendable>(
            within limit: Duration,
            on executor: (any TaskExecutor)? = nil,
            expired: (@Sendable () -> Void)? = nil,
            _ operation: sending @escaping @isolated(any) () async -> Answer
        ) async -> Answer? {
            guard limit > .zero else {
                expired?()
                return nil
            }
            let ending = Ending<Answer>()
            let timer = Timer(after: limit) {
                if ending.end(with: nil) { expired?() }
            }
            defer { timer.cancel() }
            Task(executorPreference: executor) {
                _ = ending.end(with: await operation())
            }
            return await withTaskCancellationHandler {
                await withCheckedContinuation { ending.begin($0) }
            } onCancel: {
                _ = ending.end(with: nil)
            }
        }

        /// Waits `limit` of wall time, or until the waiting task is cancelled. A limit of no time or less returns
        /// without suspending.
        public static func pause(_ limit: Duration) async {
            guard limit > .zero else { return }
            let ending = Ending<Never>()
            let timer = Timer(after: limit) { _ = ending.end(with: nil) }
            defer { timer.cancel() }
            _ = await withTaskCancellationHandler {
                await withCheckedContinuation { ending.begin($0) }
            } onCancel: {
                _ = ending.end(with: nil)
            }
        }
    }
#endif

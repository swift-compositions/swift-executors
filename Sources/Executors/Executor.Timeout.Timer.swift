#if canImport(Dispatch)
    import Dispatch
    import Synchronization

    extension Executor.Timeout {
        /// Calls a function once, when a time has passed on the wall clock, unless it is cancelled first.
        ///
        /// It fires on a serial dispatch queue that all timers share, which dispatch serves with a thread of its own
        /// however busy the cooperative pool is. Firing and cancelling exclude each other: exactly one of them
        /// happens, and `cancel()` says which.
        public final class Timer: Sendable {
            let phase: Phase

            /// Arms a timer that calls `fire` once `limit` has passed on the wall clock; a limit of no time or less
            /// fires as soon as the queue runs it.
            public init(after limit: Duration, _ fire: @escaping @Sendable () -> Void) {
                let phase = Phase()
                self.phase = phase
                Self.queue.asyncAfter(wallDeadline: .now() + .nanoseconds(Self.nanoseconds(limit))) {
                    if phase.claim(Phase.fired) { fire() }
                }
            }
        }
    }

    extension Executor.Timeout.Timer {
        /// The queue every timer fires on.
        static let queue = DispatchQueue(label: "Executor.Timeout.Timer")

        /// A duration as whole nanoseconds for dispatch, held to what an `Int` counts and to no less than zero.
        static func nanoseconds(_ limit: Duration) -> Int {
            let (seconds, attoseconds) = limit.components
            guard seconds >= 0, attoseconds >= 0 else { return 0 }
            let (whole, wide) = Int(clamping: seconds).multipliedReportingOverflow(by: 1_000_000_000)
            let (sum, long) = whole.addingReportingOverflow(Int(attoseconds / 1_000_000_000))
            return wide || long ? .max : sum
        }

        /// Stops the timer before it fires: true when this stopped it, false when it had already fired or been
        /// cancelled. Its turn on the queue still comes, and does nothing.
        @discardableResult
        public func cancel() -> Bool {
            phase.claim(Phase.cancelled)
        }

        /// Whether the timer fired.
        public var fired: Bool { phase.value.load(ordering: .acquiring) == Phase.fired }
    }
#endif

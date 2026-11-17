#if canImport(Dispatch)
    import Synchronization

    extension Executor.Timeout.Timer {
        /// Armed, fired or cancelled; the first of the last two to claim it wins.
        final class Phase: Sendable {
            let value = Atomic<UInt8>(Phase.armed)
        }
    }

    extension Executor.Timeout.Timer.Phase {
        static var armed: UInt8 { 0 }
        static var fired: UInt8 { 1 }
        static var cancelled: UInt8 { 2 }

        /// Moves an armed timer to `outcome`: true when this did, false when it was no longer armed.
        func claim(_ outcome: UInt8) -> Bool {
            value.compareExchange(expected: Self.armed, desired: outcome, ordering: .acquiringAndReleasing).exchanged
        }
    }
#endif

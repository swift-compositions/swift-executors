//
//  Kernel.Thread.Executor.Completion.Outcome.swift
//  swift-executors
//

#if !os(Windows)

    extension Kernel.Thread.Executor.Completion {
        /// Outcome returned from the tick body.
        public enum Outcome: Sendable {
            /// Continue the run loop.
            case `continue`
            /// Halt the run loop.
            case halt
        }
    }

#endif

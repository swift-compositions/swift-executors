#if !os(Windows)

    extension Kernel.Thread.Executor.Completion {

        public enum Outcome: Sendable {

            case `continue`

            case halt
        }
    }

#endif

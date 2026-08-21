#if !os(Windows)

    extension Kernel.Thread.Executor.Polling {

        public enum Outcome: Sendable {

            case `continue`

            case halt
        }
    }

#endif

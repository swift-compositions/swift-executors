extension Executor {

    public enum Runtime: Sendable, Equatable {

        case running

        case draining

        case terminated
    }
}

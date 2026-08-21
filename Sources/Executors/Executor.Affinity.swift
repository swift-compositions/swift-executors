extension Executor {

    public struct Affinity: Sendable, Equatable {

        public let index: Int

        public let count: Int

        public init(index: Int, count: Int) {
            precondition(count > 0, "Executor.Affinity: count must be positive")
            precondition(
                (0..<count).contains(index),
                "Executor.Affinity: index out of range"
            )
            self.index = index
            self.count = count
        }
    }
}

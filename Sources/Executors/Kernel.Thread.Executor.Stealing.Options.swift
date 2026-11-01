extension Kernel.Thread.Executor.Stealing {

    public struct Options: Sendable {

        public var count: Kernel.Thread.Count

        public var priorityTracking: Bool

        public init(
            count: Kernel.Thread.Count? = nil,
            priorityTracking: Bool = false
        ) {
            self.count =
                count
                ?? Kernel.Thread.Count.min(
                    Self.defaultCount,
                    Kernel.Thread.Count(Int(CPU.Logical.count))
                )
            self.priorityTracking = priorityTracking
        }
    }
}

extension Kernel.Thread.Executor.Stealing.Options {
    private static let defaultCount: Kernel.Thread.Count = 4
}

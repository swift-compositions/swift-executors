extension Kernel.Thread.Executor.Sharded {

    public var runtime: Executor.Runtime {
        let states = executors.map(\.runtime)
        if states.contains(.running) { return .running }
        if states.contains(.draining) { return .draining }
        return .terminated
    }

    public var affinities: [Executor.Affinity] {
        let total = executors.count
        return (0..<total).map { Executor.Affinity(index: $0, count: total) }
    }
}

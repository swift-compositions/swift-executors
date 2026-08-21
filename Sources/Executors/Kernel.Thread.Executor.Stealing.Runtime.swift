extension Kernel.Thread.Executor.Stealing.Worker {

    fileprivate var runtime: Executor.Runtime {
        wait.withLock {
            loopExited ? .terminated : .running
        }
    }
}

extension Kernel.Thread.Executor.Stealing {

    public var runtime: Executor.Runtime {
        if workers.allSatisfy({ $0.runtime == .terminated }) { return .terminated }
        return _shutdown.isSet ? .draining : .running
    }

    public var affinities: [Executor.Affinity] {
        let total = workers.count
        return (0..<total).map { Executor.Affinity(index: $0, count: total) }
    }
}

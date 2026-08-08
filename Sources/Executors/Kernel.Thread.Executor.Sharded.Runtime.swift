//
//  Kernel.Thread.Executor.Sharded.Runtime.swift
//  swift-executors
//
//  TX-N1D — additive `Executor.Runtime`/`Executor.Affinity` accessors
//  for the sharded pool. Both read state the pool already exposes
//  per-shard (via `Kernel.Thread.Executor.runtime`) or already assigns
//  at construction (the shard's round-robin position); neither changes
//  `Sharded`'s documented lifecycle or shutdown behavior.
//

extension Kernel.Thread.Executor.Sharded {
    /// This pool's aggregate progress state.
    ///
    /// `.running` if any shard is still running, `.draining` if none are
    /// running but at least one has not yet exited its run loop, and
    /// `.terminated` only once every shard has verifiably exited --
    /// mirroring `shutdown()`'s guarantee that it blocks until all
    /// shards have terminated.
    public var runtime: Executor.Runtime {
        let states = executors.map(\.runtime)
        if states.contains(.running) { return .running }
        if states.contains(.draining) { return .draining }
        return .terminated
    }

    /// This pool's shard affinities, in round-robin order.
    ///
    /// Each element names the position `Sharded` already assigns a shard
    /// via `executor(at:)`/round-robin dispatch; this does not change
    /// shard selection.
    public var affinities: [Executor.Affinity] {
        let total = executors.count
        return (0..<total).map { Executor.Affinity(index: $0, count: total) }
    }
}

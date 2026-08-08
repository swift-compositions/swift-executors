//
//  Kernel.Thread.Executor.Stealing.Runtime.swift
//  swift-executors
//
//  TX-N1D — additive `Executor.Runtime`/`Executor.Affinity` accessors
//  for the work-stealing pool. Both read state each `Worker` already
//  tracks per the Teardown Contract applied per worker (see
//  `Kernel.Thread.Executor.Stealing.Worker`'s own documentation);
//  neither changes stealing, dispatch, or shutdown behavior.
//

extension Kernel.Thread.Executor.Stealing.Worker {
    /// This worker's current progress state, read under its own lock.
    fileprivate var runtime: Executor.Runtime {
        wait.withLock {
            loopExited ? .terminated : .running
        }
    }
}

extension Kernel.Thread.Executor.Stealing {
    /// This pool's aggregate progress state.
    ///
    /// `.running` if any worker has not yet exited its run loop,
    /// `.terminated` once every worker has. Unlike `Sharded`, a
    /// `Stealing` pool has no intermediate pool-wide draining signal
    /// distinct from "shutdown requested" without also inspecting every
    /// worker's deque, so `.draining` is reported for the window between
    /// `shutdown()` being signalled and the last worker's loop exit,
    /// mirroring the pool-level `_shutdown` flag rather than aggregating
    /// per-worker deque state.
    public var runtime: Executor.Runtime {
        if workers.allSatisfy({ $0.runtime == .terminated }) { return .terminated }
        return _shutdown.isSet ? .draining : .running
    }

    /// This pool's worker affinities, in worker-id order.
    ///
    /// Each element names the stable identity `Stealing` already assigns
    /// a worker at construction (`Worker.id`); this does not change
    /// which worker executes or steals a given job.
    public var affinities: [Executor.Affinity] {
        let total = workers.count
        return (0..<total).map { Executor.Affinity(index: $0, count: total) }
    }
}

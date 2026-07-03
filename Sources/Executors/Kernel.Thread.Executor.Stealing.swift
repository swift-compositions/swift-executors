//
//  Kernel.Thread.Executor.Stealing.swift
//  swift-executors
//

import Index_Primitives
import Ordinal_Primitives
import Synchronization

extension Kernel.Thread.Executor {
    /// N-owned-threads with per-thread deques and work-stealing.
    ///
    /// Each worker owns its `Executor.Job.Deque`. Workers steal from each other
    /// when their own deque is empty. Unlike `Sharded`, jobs are not pinned to a
    /// specific thread -- any worker can run any job -- so only `TaskExecutor`
    /// conformance is appropriate (stealing violates serial execution order).
    ///
    /// ## Safety Invariant
    ///
    /// This type is `Sendable` by virtue of internal synchronization. The
    /// cross-worker state it owns is limited to:
    /// - `cursor: Atomic<Index<Kernel.Thread>>` -- round-robin dispatcher
    ///   index, mutated only by `advance(within:)`.
    /// - `_shutdown: Shutdown.Flag` -- atomic boolean.
    /// - `workers: [Worker]` -- an immutable-after-init array of
    ///   independently-synchronized `Worker` instances (see the Worker type's
    ///   own safety invariant).
    ///
    /// All producer/enqueue paths hit the atomic cursor and then a per-worker
    /// condvar. The caller MUST interact only through the public API
    /// (`enqueue`, `shutdown`, the unowned-task-executor accessor); touching
    /// `workers` or `cursor` directly is undefined behaviour.
    ///
    /// ## Intended Use
    ///
    /// - Fan-out task execution across N OS threads with automatic load
    ///   balancing via work-stealing.
    /// - `withTaskExecutorPreference` for CPU-bound parallel workloads where
    ///   serial ordering is NOT required.
    /// - Default "general pool" task executor at the kernel-thread layer.
    ///
    /// ## Non-Goals
    ///
    /// - Not a SerialExecutor. Stealing violates serial execution order;
    ///   Swift actor semantics cannot be honored here.
    /// - Not safe to shutdown from a worker thread. Must be called from
    ///   outside the pool.
    /// - Not a substitute for `Kernel.Thread.Executor` when actor pinning is
    ///   required.
    ///
    /// ## Lifecycle
    /// Call `shutdown()` before deallocation. Must not be called from a worker thread.
    public final class Stealing: TaskExecutor, @unsafe @unchecked Sendable {
        internal let workers: [Worker]
        internal let _shutdown: Executor_Primitives.Executor.Shutdown.Flag
        internal let priorityTracking: Bool
        private let cursor: Atomic<Index<Kernel.Thread>>
        public let count: Kernel.Thread.Count

        public init(_ options: Options = .init()) {
            self.count = options.count
            self._shutdown = .init()
            self.priorityTracking = options.priorityTracking
            self.cursor = .init(.zero)
            self.workers = Array(count: options.count) { position in
                Worker(id: position)
            }
            for worker in workers {
                worker.start(pool: self)
            }
        }
    }
}

// MARK: - TaskExecutor

extension Kernel.Thread.Executor.Stealing {
    public func enqueue(_ job: consuming ExecutorJob) {
        enqueue(UnownedJob(job))
    }

    public func enqueue(_ job: UnownedJob) {
        workers[cursor.advance(within: count)].enqueue(job)
    }

    public func asUnownedTaskExecutor() -> UnownedTaskExecutor {
        unsafe UnownedTaskExecutor(ordinary: self)
    }
}

// MARK: - Shutdown

extension Kernel.Thread.Executor.Stealing {
    /// Shutdown all worker threads.
    ///
    /// Signals each worker's run loop to exit, wakes all, then joins all threads.
    public func shutdown() {
        _shutdown.set()
        for worker in workers { worker.wake() }
        for worker in workers { worker.join() }
    }
}

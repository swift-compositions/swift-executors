//
//  Kernel.Thread.Executor.Stealing.Worker.swift
//  swift-executors
//

import Index_Primitives
import Ordinal_Primitives

extension Kernel.Thread.Executor.Stealing {
    /// A single work-stealing worker owning one OS thread and one deque.
    ///
    /// ## Safety Invariant
    ///
    /// This type is `Sendable` by virtue of internal synchronization: the deque
    /// (`deque`), the loop-exited flag (`loopExited`), and the thread handle
    /// (`handle`) are mutated exclusively
    /// under `wait: Executor.Wait.Condvar`. The enqueue / pop / steal / wake
    /// / join paths all serialize through `wait.withLock`. Cross-worker steal
    /// attempts touch the victim's deque under the victim's own `wait` lock --
    /// never under the stealer's. The caller (the parent `Stealing` pool)
    /// MUST route all operations through the package-visible API.
    ///
    /// ## Teardown
    ///
    /// Applies the `Kernel.Thread.Executor` Teardown Contract per worker:
    /// `loopExited` is published in the same critical section as the
    /// empty-deque observation that ends the post-shutdown drain, and
    /// `enqueue` rejects jobs (returning `false`) once it is set, so a job
    /// can never be pushed onto a deque no thread will ever drain.
    ///
    /// ## Intended Use
    ///
    /// - Internal building block of `Kernel.Thread.Executor.Stealing` --
    ///   one Worker per OS thread in the pool.
    /// - Hosts the work-stealing run loop: drain own deque, then attempt to
    ///   steal from peer workers, then block on condvar.
    ///
    /// ## Non-Goals
    ///
    /// - Not a public API. Consumers use `Kernel.Thread.Executor.Stealing`,
    ///   not `Worker` directly.
    /// - Not safe to use outside a `Stealing` pool -- lifetime and shutdown
    ///   semantics are owned by the pool.
    package final class Worker: @unsafe @unchecked Sendable {
        let id: Index<Kernel.Thread>
        private var deque: Executor_Primitives.Executor.Job.Deque
        private let wait: Executor.Wait.Condvar
        /// `true` once `runLoop(pool:)` has finished its post-shutdown
        /// drain. Guarded by `wait`; set in the same critical section as
        /// the empty-deque observation that ends the drain. See
        /// ``Teardown``.
        private var loopExited: Bool
        private var handle: Kernel.Thread.Handle?
        /// Per-worker XorShift32 state for random victim selection.
        /// Mutated only from this worker's own runLoop (single-writer),
        /// so no synchronization needed.
        private var rngState: UInt32

        init(id: Index<Kernel.Thread>) {
            self.id = id
            self.deque = .init(capacity: 1024)
            self.wait = .init()
            self.loopExited = false
            // XorShift32 requires non-zero state; OR with 1 guarantees it.
            self.rngState = UInt32(truncatingIfNeeded: id.ordinal.rawValue) &+ 0x9E37_79B9
            if self.rngState == 0 { self.rngState = 1 }
        }
    }
}

// MARK: - PRNG

extension Kernel.Thread.Executor.Stealing.Worker {
    /// Advance the XorShift32 PRNG and return the next 32-bit value.
    ///
    /// From Marsaglia 2003: period = 2^32 − 1, non-zero state.
    /// One multiplication-free mix per call; dominated by the three
    /// shifts on modern hardware.
    private func nextRandom() -> UInt32 {
        rngState ^= rngState &<< 13
        rngState ^= rngState &>> 17
        rngState ^= rngState &<< 5
        return rngState
    }
}

// MARK: - Lifecycle

extension Kernel.Thread.Executor.Stealing.Worker {
    func start(pool: Kernel.Thread.Executor.Stealing) {
        self.handle = unsafe Kernel.Thread.trap(Ownership.Transfer.Retained<Kernel.Thread.Executor.Stealing.Worker>.Outgoing(self)) { retained in
            let worker = retained.consume()
            worker.runLoop(pool: pool)
        }
    }

    func wake() { wait.wake.all() }

    func join() throws(Kernel.Thread.Error) {
        try handle.take()?.join()
    }
}

// MARK: - Job Queue

extension Kernel.Thread.Executor.Stealing.Worker {
    /// Push a job onto this worker's deque.
    ///
    /// - Returns: `true` if the job was accepted (a worker thread will
    ///   execute it), `false` if this worker's run loop has fully exited
    ///   post-shutdown and can never execute it -- the caller must run
    ///   the job itself (see the `Kernel.Thread.Executor` Teardown
    ///   Contract).
    func enqueue(_ job: UnownedJob) -> Bool {
        let accepted: Bool = wait.withLock {
            guard !loopExited else { return false }
            _ = deque.push(job)
            return true
        }
        if accepted { wait.wake() }
        return accepted
    }

    fileprivate func trySteal() -> UnownedJob? {
        wait.withLock { deque.steal() }
    }
}

// MARK: - Run Loop

extension Kernel.Thread.Executor.Stealing.Worker {
    private func runLoop(pool: Kernel.Thread.Executor.Stealing) {
        while !pool._shutdown.isSet {
            // Own deque — under own lock
            if let job = wait.withLock({ deque.take() }) {
                unsafe Kernel.Thread.Executor.runJob(
                    job,
                    onTask: pool.asUnownedTaskExecutor(),
                    priorityTracking: pool.priorityTracking
                )
                continue
            }
            // Steal — NOT under own lock, only victim's.
            // Random victim selection via per-worker XorShift32 PRNG
            // per work-stealing-scheduler-design.md Q2. Up to N-1
            // attempts; each attempt uniformly samples a non-self
            // peer.
            var stolen: UnownedJob? = nil
            let count = pool.count
            if count > .one {
                let limit = count.subtract.saturating(.one)
                var attempts = Index<Kernel.Thread>.zero
                while attempts < limit {
                    var victim = Index<Kernel.Thread>(Ordinal(UInt(nextRandom()))) % count
                    if victim == id {
                        victim = (victim + .one) % count
                    }
                    if let job = pool.workers[victim].trySteal() {
                        stolen = job
                        break
                    }
                    attempts += .one
                }
            }
            if let job = stolen {
                unsafe Kernel.Thread.Executor.runJob(
                    job,
                    onTask: pool.asUnownedTaskExecutor(),
                    priorityTracking: pool.priorityTracking
                )
                continue
            }
            // Wait — under own lock
            wait.withLock {
                if !pool._shutdown.isSet && deque.isEmpty {
                    wait.wait()
                }
            }
        }
        // Drain remaining. Teardown Contract: the empty-deque observation
        // that ends the drain and the `loopExited` publication happen in
        // ONE critical section, so an enqueue serialized before it is
        // drained here, and one serialized after is rejected (and run
        // inline by the pool). No window exists in which a job can be
        // pushed yet never run.
        while true {
            let job: UnownedJob? = wait.withLock {
                if let job = deque.take() { return job }
                loopExited = true
                return nil
            }
            guard let job else { return }
            unsafe Kernel.Thread.Executor.runJob(
                job,
                onTask: pool.asUnownedTaskExecutor(),
                priorityTracking: pool.priorityTracking
            )
        }
    }
}

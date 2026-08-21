import Index_Primitives
import Ordinal_Primitives

extension Kernel.Thread.Executor.Stealing {

    package final class Worker: @unsafe @unchecked Sendable {
        let id: Index<Kernel.Thread>
        private var deque: Executor_Primitives.Executor.Job.Deque

        internal let wait: Executor.Wait.Condvar

        internal var loopExited: Bool
        private var handle: Kernel.Thread.Handle?

        private var rngState: UInt32

        init(id: Index<Kernel.Thread>) {
            self.id = id
            self.deque = .init(capacity: 1024)
            self.wait = .init()
            self.loopExited = false

            self.rngState = UInt32(truncatingIfNeeded: id.ordinal.rawValue) &+ 0x9E37_79B9
            if self.rngState == 0 { self.rngState = 1 }
        }
    }
}

extension Kernel.Thread.Executor.Stealing.Worker {

    private func nextRandom() -> UInt32 {
        rngState ^= rngState &<< 13
        rngState ^= rngState &>> 17
        rngState ^= rngState &<< 5
        return rngState
    }
}

extension Kernel.Thread.Executor.Stealing.Worker {
    func start(pool: Kernel.Thread.Executor.Stealing) {
        self.handle = unsafe Kernel.Thread.trap(
            Ownership.Transfer.Retained<Kernel.Thread.Executor.Stealing.Worker>.Outgoing(self)
        ) { retained in
            let worker = retained.consume()
            worker.runLoop(pool: pool)
        }
    }

    func wake() { wait.wake.all() }

    func join() throws(Kernel.Thread.Error) {
        try handle.take()?.join()
    }
}

extension Kernel.Thread.Executor.Stealing.Worker {

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

extension Kernel.Thread.Executor.Stealing.Worker {
    private func runLoop(pool: Kernel.Thread.Executor.Stealing) {
        while !pool._shutdown.isSet {

            if let job = wait.withLock({ deque.take() }) {
                unsafe Kernel.Thread.Executor.runJob(
                    job,
                    onTask: pool.asUnownedTaskExecutor(),
                    priorityTracking: pool.priorityTracking
                )
                continue
            }

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

            wait.withLock {
                if !pool._shutdown.isSet && deque.isEmpty {
                    wait.wait()
                }
            }
        }

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

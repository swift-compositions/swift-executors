import Index
import Ordinal

extension Kernel.Thread.Executor.Stealing {

    package final class Worker: @unsafe @unchecked Sendable {
        let id: Index<Kernel.Thread>
        private var deque: Executor::Executor.Job.Deque

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
    /// Starts this worker's thread, which holds the crew, never the pool.
    func start(crew: Kernel.Thread.Executor.Stealing.Crew) {
        let id = self.id
        self.handle = unsafe Kernel.Thread.trap(
            Ownership.Transfer.Retained<Kernel.Thread.Executor.Stealing.Crew>.Outgoing(crew)
        ) { retained in
            let crew = retained.consume()
            crew.workers[id].runLoop(crew: crew)
        }
    }

    func wake() { wait.wake.all() }

    func join() throws(Kernel.Thread.Error) {
        try handle.take()?.join()
    }

    /// Lets the thread end by itself, unjoined.
    func detach() {
        do throws(Kernel.Thread.Error) {
            try handle.take()?.detach()
        } catch {
        }
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
    fileprivate func runLoop(crew: Kernel.Thread.Executor.Stealing.Crew) {
        while !crew.shutdown.isSet {

            if let job = wait.withLock({ deque.take() }) {
                crew.run(job)
                continue
            }

            var stolen: UnownedJob? = nil
            let count = crew.count
            if count > .one {
                let limit = count.subtract.saturating(.one)
                var attempts = Index<Kernel.Thread>.zero
                while attempts < limit {
                    var victim = Index<Kernel.Thread>(Ordinal(UInt(nextRandom()))) % count
                    if victim == id {
                        victim = (victim + .one) % count
                    }
                    if let job = crew.workers[victim].trySteal() {
                        stolen = job
                        break
                    }
                    attempts += .one
                }
            }
            if let job = stolen {
                crew.run(job)
                continue
            }

            wait.withLock {
                if !crew.shutdown.isSet && deque.isEmpty {
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
            crew.run(job)
        }
    }
}

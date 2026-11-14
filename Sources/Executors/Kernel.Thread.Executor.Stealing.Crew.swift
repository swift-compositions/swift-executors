extension Kernel.Thread.Executor.Stealing {

    /// What a stealing pool's threads share: its workers, its shutdown flag and how jobs run.
    ///
    /// The threads hold the crew and never the pool, so the pool's last reference can go while they wait for
    /// work; its `deinit` then sets the flag, wakes them, and they end. A job runs only while something holds
    /// the pool, so the crew reaches it through an unowned reference.
    final class Crew {
        let workers: [Worker]
        let shutdown: Executor::Executor.Shutdown.Flag
        let count: Kernel.Thread.Count
        let priorityTracking: Bool

        unowned var pool: Kernel.Thread.Executor.Stealing?

        init(workers: [Worker], count: Kernel.Thread.Count, priorityTracking: Bool) {
            self.workers = workers
            self.shutdown = .init()
            self.count = count
            self.priorityTracking = priorityTracking
            self.pool = nil
        }
    }
}

extension Kernel.Thread.Executor.Stealing.Crew {
    /// Sets the flag and wakes every worker, so each ends once its queue is empty.
    func end() {
        shutdown.set()
        for worker in workers { worker.wake() }
    }

    /// Runs a job on the pool, while there is one.
    func run(_ job: UnownedJob) {
        pool?.run(job)
    }
}

extension Kernel.Thread.Executor {

    /// What an executor's thread runs: its queue of jobs, the wait on it and the shutdown flag.
    ///
    /// The thread holds the loop and never the executor, so the executor's last reference can go while its
    /// thread waits for work; its `deinit` then sets the flag and the thread ends. A job runs only while
    /// something holds the executor (a task that prefers it, an actor on it), so the loop reaches the
    /// executor through an unowned reference.
    final class Loop {

        let mode: Mode
        let priorityTracking: Bool

        let wait: Executor::Executor.Wait.Condvar
        var jobs: Executor::Executor.Job.Queue
        let shutdown: Executor::Executor.Shutdown.Flag

        var exited: Bool

        unowned var executor: Kernel.Thread.Executor?

        init(mode: Mode, priorityTracking: Bool) {
            self.mode = mode
            self.priorityTracking = priorityTracking
            self.wait = .init()
            self.jobs = .init()
            self.shutdown = .init()
            self.exited = false
            self.executor = nil
        }
    }
}

extension Kernel.Thread.Executor.Loop {

    /// Takes the next job, waiting while there is none, until the flag is set and the queue is empty.
    func run() {
        while true {
            let job: UnownedJob? = wait.withLock {
                while jobs.isEmpty && !shutdown.isSet {
                    wait.wait()
                }
                guard !shutdown.isSet || !jobs.isEmpty else {
                    exited = true
                    return nil
                }
                return jobs.dequeue()
            }
            guard let job, let executor else { return }
            executor.run(job)
        }
    }

    /// Sets the flag and wakes the thread, which ends once the queue is empty.
    func end() {
        wait.withLock {
            shutdown.set()
        }
        wait.wake.all()
    }
}

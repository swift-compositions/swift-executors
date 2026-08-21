extension Kernel.Thread {

    public final class Executor: SerialExecutor, TaskExecutor, @unsafe @unchecked Sendable {

        private let mode: Mode
        private let priorityTracking: Bool

        internal let wait: Executor_Primitives.Executor.Wait.Condvar
        private var jobs: Executor_Primitives.Executor.Job.Queue
        internal let _shutdown: Executor_Primitives.Executor.Shutdown.Flag

        internal var _loopExited: Bool
        private var threadHandle: Kernel.Thread.Handle?

        public init(mode: Mode = .serial, priorityTracking: Bool = false) {
            self.mode = mode
            self.priorityTracking = priorityTracking
            self.wait = .init()
            self.jobs = .init()
            self._shutdown = .init()
            self._loopExited = false

            self.threadHandle = unsafe Kernel.Thread.trap(
                Ownership.Transfer.Retained<Kernel.Thread.Executor>.Outgoing(self)
            ) { retained in
                let executor = retained.consume()
                executor.runLoop()
            }
        }

        deinit {
            guard let handle = threadHandle.take() else { return }
            wait.withLock {
                _shutdown.set()
            }
            wait.wake.all()

            do throws(Kernel.Thread.Error) {
                try handle.detach()
            } catch {
            }
        }
    }
}

extension Kernel.Thread.Executor {
    public func enqueue(_ job: UnownedJob) {

        let runInline: Bool = wait.withLock {
            guard !_loopExited else { return true }
            jobs.enqueue(job)
            return false
        }
        if runInline {
            switch mode {
            case .serial:
                unsafe Self.runJob(
                    job,
                    onSerial: asUnownedSerialExecutor(),
                    priorityTracking: priorityTracking
                )

            case .task:
                unsafe Self.runJob(
                    job,
                    onTask: asUnownedTaskExecutor(),
                    priorityTracking: priorityTracking
                )
            }
        } else {
            wait.wake()
        }
    }

    public func asUnownedSerialExecutor() -> UnownedSerialExecutor {
        unsafe UnownedSerialExecutor(ordinary: self)
    }
}

extension Kernel.Thread.Executor {

    public func isIsolatingCurrentContext() -> Bool? {
        threadHandle?.isCurrent
    }

    public func checkIsolated() {
        guard isIsolatingCurrentContext() == true else {
            preconditionFailure(
                "Kernel.Thread.Executor: expected current thread to be the executor's thread"
            )
        }
    }
}

extension Kernel.Thread.Executor {
    public func enqueue(_ job: consuming ExecutorJob) {
        enqueue(UnownedJob(job))
    }
}

extension Kernel.Thread.Executor {
    fileprivate func runLoop() {
        while true {
            let job: UnownedJob? = wait.withLock {
                while jobs.isEmpty && !_shutdown.isSet {
                    wait.wait()
                }
                guard !_shutdown.isSet || !jobs.isEmpty else {

                    _loopExited = true
                    return nil
                }
                return jobs.dequeue()
            }
            guard let job else { return }
            switch mode {
            case .serial:
                unsafe Self.runJob(
                    job,
                    onSerial: asUnownedSerialExecutor(),
                    priorityTracking: priorityTracking
                )

            case .task:
                unsafe Self.runJob(
                    job,
                    onTask: asUnownedTaskExecutor(),
                    priorityTracking: priorityTracking
                )
            }
        }
    }
}

extension Kernel.Thread.Executor {

    public func shutdown() {
        guard let handle = threadHandle.take() else {
            preconditionFailure(
                "Kernel.Thread.Executor.shutdown() called on already-shutdown or never-started executor"
            )
        }

        wait.withLock {
            _shutdown.set()
        }
        wait.wake.all()

        if handle.isCurrent {

            do throws(Kernel.Thread.Error) {
                try handle.detach()
            } catch {
            }
        } else {

            do throws(Kernel.Thread.Error) {
                try handle.join()
            } catch {
            }
        }
    }
}

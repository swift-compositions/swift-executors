extension Executor {

    public final class Cooperative: SerialExecutor, @unsafe @unchecked Sendable {
        private var jobs: Executor.Job.Queue
        private var drainBuffer: Executor.Job.Queue

        private let wait: Executor.Wait.Condvar
        private let _shutdown: Executor.Shutdown.Flag

        private var _stopped: Bool

        private var _isRunning: Bool

        public init() {
            self.jobs = .init()
            self.drainBuffer = .init()

            self.wait = .init()
            self._shutdown = .init()
            self._stopped = false
            self._isRunning = false
        }
    }
}

extension Executor.Cooperative {
    public func enqueue(_ job: consuming ExecutorJob) {
        enqueue(UnownedJob(job))
    }

    public func enqueue(_ job: UnownedJob) {
        let accepted: Bool = wait.withLock {
            guard !_shutdown.isSet else { return false }
            jobs.enqueue(job)
            return true
        }
        if accepted {
            wait.wake()
            return
        }

        assertionFailure(
            "Executor.Cooperative.enqueue(_:) after shutdown(): job abandoned — its continuation will never resume"
        )
    }

    public func asUnownedSerialExecutor() -> UnownedSerialExecutor {
        unsafe UnownedSerialExecutor(ordinary: self)
    }
}

extension Executor.Cooperative {

    public func run() {
        runUntil { false }
    }

    public func runUntil(_ condition: () -> Bool) {
        precondition(!_isRunning, "nested runUntil is not supported")
        _isRunning = true
        wait.withLock { _stopped = false }
        defer { _isRunning = false }

        while !_shutdown.isSet {
            if condition() { return }

            let shouldExit = wait.withLock { () -> Bool in

                while jobs.isEmpty && !_shutdown.isSet && !_stopped {

                    wait.wait()

                }
                if _stopped || _shutdown.isSet { return true }
                swap(&jobs, &drainBuffer)
                return false
            }

            if shouldExit { return }

            while let job = drainBuffer.dequeue() {
                unsafe job.runSynchronously(on: asUnownedSerialExecutor())
            }
        }
    }

    public func stop() {
        wait.withLock { _stopped = true }
        wait.wake.all()
    }

    public func shutdown() {
        _shutdown.set()
        wait.wake.all()
    }
}

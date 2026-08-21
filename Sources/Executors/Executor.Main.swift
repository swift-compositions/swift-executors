#if os(macOS) || os(iOS) || os(tvOS) || os(watchOS) || os(visionOS)
    import Dispatch
#endif

extension Executor {

    public final class Main: SerialExecutor, @unsafe @unchecked Sendable {
        #if !(os(macOS) || os(iOS) || os(tvOS) || os(watchOS) || os(visionOS))
            private var jobs: Executor.Job.Queue
            private var drainBuffer: Executor.Job.Queue

            private let wait: Executor.Wait.Condvar
            private let _shutdown: Executor.Shutdown.Flag
            private var _stopped: Bool
            private var _isRunning: Bool
        #endif

        private init() {
            #if !(os(macOS) || os(iOS) || os(tvOS) || os(watchOS) || os(visionOS))
                self.jobs = .init()
                self.drainBuffer = .init()

                self.wait = .init()
                self._shutdown = .init()
                self._stopped = false
                self._isRunning = false
            #endif
        }
    }
}

extension Executor.Main {

    public static let shared: Executor.Main = .init()
}

extension Executor.Main {
    public func enqueue(_ job: consuming ExecutorJob) {
        let unowned = UnownedJob(job)
        #if os(macOS) || os(iOS) || os(tvOS) || os(watchOS) || os(visionOS)
            DispatchQueue.main.async {
                unsafe unowned.runSynchronously(
                    on: self.asUnownedSerialExecutor()
                )
            }
        #else
            let accepted: Bool = wait.withLock {
                guard !_shutdown.isSet else { return false }
                jobs.enqueue(unowned)
                return true
            }
            if accepted {
                wait.wake()
                return
            }

            assertionFailure(
                "Executor.Main.enqueue(_:) after shutdown(): job abandoned — its continuation will never resume"
            )
        #endif
    }

    public func asUnownedSerialExecutor() -> UnownedSerialExecutor {
        unsafe UnownedSerialExecutor(ordinary: self)
    }
}

#if !(os(macOS) || os(iOS) || os(tvOS) || os(watchOS) || os(visionOS))
    extension Executor.Main {

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
#endif

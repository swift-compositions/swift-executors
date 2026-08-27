#if !os(Windows)

    extension Kernel.Thread.Executor {

        @safe
        public final class Polling: SerialExecutor, TaskExecutor, @unsafe @unchecked Sendable {

            private var jobs: Executor.Executor.Job.Queue
            private var drainBuffer: Executor.Executor.Job.Queue
            private let queueLock: Kernel.Thread.Mutex
            private var waitSource: Executor.Executor.Wait.Event.Source
            private let _shutdown: Executor.Executor.Shutdown.Flag

            private var _loopExited: Bool
            private var threadHandle: Kernel.Thread.Handle?
            private let maxEventsPerPoll: Int
            private let priorityTracking: Bool
            private let tick:
                (
                    () throws(Kernel.Event.Driver.Error) -> UnsafeBufferPointer<Kernel.Event>
                ) -> Outcome

            public init(
                source: consuming Kernel.Event.Source,
                maxEventsPerPoll: Int = 256,
                priorityTracking: Bool = false,
                tick:
                    sending @escaping (
                        () throws(Kernel.Event.Driver.Error) -> UnsafeBufferPointer<Kernel.Event>
                    ) -> Outcome
            ) {
                self.jobs = .init()
                self.drainBuffer = .init()
                self.queueLock = .init()
                self.waitSource = .init(source: consume source)
                self._shutdown = .init()
                self._loopExited = false
                self.maxEventsPerPoll = maxEventsPerPoll
                self.priorityTracking = priorityTracking
                unsafe (self.tick = tick)
                self.threadHandle = unsafe Kernel.Thread.trap(
                    Ownership.Transfer.Retained<Kernel.Thread.Executor.Polling>.Outgoing(self)
                ) { retained in
                    retained.consume().runLoop()
                }
            }
        }
    }

    extension Kernel.Thread.Executor.Polling {
        public func enqueue(_ job: consuming ExecutorJob) {
            enqueue(UnownedJob(job))
        }

        public func enqueue(_ job: UnownedJob) {

            let runInline: Bool = queueLock.withLock {
                guard !_loopExited else { return true }
                jobs.enqueue(job)
                return false
            }
            if runInline {
                unsafe Kernel.Thread.Executor.runJob(
                    job,
                    onSerial: asUnownedSerialExecutor(),
                    priorityTracking: priorityTracking
                )
            } else {
                waitSource.wakeup.wake()
            }
        }

        public func asUnownedSerialExecutor() -> UnownedSerialExecutor {
            unsafe UnownedSerialExecutor(ordinary: self)
        }
    }

    extension Kernel.Thread.Executor.Polling {

        public func isIsolatingCurrentContext() -> Bool? {
            threadHandle?.isCurrent
        }

        public func checkIsolated() {
            guard isIsolatingCurrentContext() == true else {
                preconditionFailure(
                    "Kernel.Thread.Executor.Polling: expected current thread to be the executor's thread"
                )
            }
        }
    }

    extension Kernel.Thread.Executor.Polling {
        public func asUnownedTaskExecutor() -> UnownedTaskExecutor {
            unsafe UnownedTaskExecutor(ordinary: self)
        }
    }

    extension Kernel.Thread.Executor.Polling {

        public var source: Kernel.Event.Source {
            _read { yield waitSource.source }
            _modify { yield &waitSource.source }
        }
    }

    extension Kernel.Thread.Executor.Polling {

        public func shutdown() {
            _shutdown.set()
            waitSource.wakeup.wake()
            if let handle = threadHandle.take() {
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
    }

    extension Kernel.Thread.Executor.Polling {
        private func runLoop() {
            var eventBuffer = [Kernel.Event](repeating: Kernel.Event.empty, count: maxEventsPerPoll)
            while !_shutdown.isSet {
                drainJobs()
                if _shutdown.isSet { break }

                let count: Int
                let waitError: Kernel.Event.Driver.Error?
                do throws(Kernel.Event.Driver.Error) {
                    count = try waitSource.wait(deadline: nil, into: &eventBuffer)
                    waitError = nil
                } catch {
                    count = 0
                    waitError = error
                }
                if _shutdown.isSet { break }

                let outcome = eventBuffer.withUnsafeBufferPointer { base in
                    unsafe tick {
                        () throws(Kernel.Event.Driver.Error) -> UnsafeBufferPointer<Kernel.Event> in
                        if let waitError { throw waitError }
                        return unsafe UnsafeBufferPointer<Kernel.Event>(
                            start: base.baseAddress,
                            count: count
                        )
                    }
                }
                if case .halt = outcome {
                    _shutdown.set()
                    break
                }
            }

            drainJobs(publishLoopExitedIfEmpty: true)
        }

        private func drainJobs(publishLoopExitedIfEmpty: Bool = false) {
            while true {
                let isEmpty: Bool = queueLock.withLock {
                    jobs.drain(into: &drainBuffer)
                    if publishLoopExitedIfEmpty && drainBuffer.isEmpty {

                        _loopExited = true
                    }
                    return drainBuffer.isEmpty
                }
                guard !isEmpty else { return }
                while let job = drainBuffer.dequeue() {
                    unsafe Kernel.Thread.Executor.runJob(
                        job,
                        onSerial: asUnownedSerialExecutor(),
                        priorityTracking: priorityTracking
                    )
                }
            }
        }
    }

#endif

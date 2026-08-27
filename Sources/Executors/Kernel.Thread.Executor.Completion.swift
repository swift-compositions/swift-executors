#if !os(Windows)

    extension Kernel.Thread.Executor {

        @safe
        public final class Completion: SerialExecutor, TaskExecutor, @unsafe @unchecked Sendable {

            private var jobs: Executor.Executor.Job.Queue
            private var drainBuffer: Executor.Executor.Job.Queue
            private let queueLock: Kernel.Thread.Mutex
            private var _kernel: Kernel.Completion?
            private let kernelWakeup: Kernel.Wakeup.Channel
            private let _shutdown: Executor.Executor.Shutdown.Flag

            private var _loopExited: Bool
            private var threadHandle: Kernel.Thread.Handle?
            private let maxCompletionsPerPoll: Int
            private let tick:
                (
                    () throws(Kernel.Completion.Error) -> UnsafeBufferPointer<
                        Kernel.Completion.Event
                    >
                ) -> Outcome

            public init(
                kernel: consuming Kernel.Completion,
                maxCompletionsPerPoll: Int = 256,
                tick:
                    sending @escaping (
                        () throws(Kernel.Completion.Error) -> UnsafeBufferPointer<
                            Kernel.Completion.Event
                        >
                    ) -> Outcome
            ) {
                self.jobs = .init()
                self.drainBuffer = .init()
                self.queueLock = .init()
                self.kernelWakeup = kernel.wakeup
                self._kernel = consume kernel
                self._shutdown = .init()
                self._loopExited = false
                self.maxCompletionsPerPoll = maxCompletionsPerPoll
                unsafe (self.tick = tick)
                self.threadHandle = unsafe Kernel.Thread.trap(
                    Ownership.Transfer.Retained<Kernel.Thread.Executor.Completion>.Outgoing(self)
                ) { retained in
                    retained.consume().runLoop()
                }
            }

            deinit {

                if let thread = threadHandle.take() {
                    _shutdown.set()
                    kernelWakeup.wake()

                    do throws(Kernel.Thread.Error) {
                        try thread.detach()
                    } catch {
                    }
                }

                if let k = _kernel.take() {
                    k.close()
                }
            }
        }
    }

    extension Kernel.Thread.Executor.Completion {
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
                unsafe job.runSynchronously(on: asUnownedSerialExecutor())
            } else {
                kernelWakeup.wake()
            }
        }

        public func asUnownedSerialExecutor() -> UnownedSerialExecutor {
            unsafe UnownedSerialExecutor(ordinary: self)
        }
    }

    extension Kernel.Thread.Executor.Completion {

        public func isIsolatingCurrentContext() -> Bool? {
            threadHandle?.isCurrent
        }

        public func checkIsolated() {
            guard isIsolatingCurrentContext() == true else {
                preconditionFailure(
                    "Kernel.Thread.Executor.Completion: expected current thread to be the executor's thread"
                )
            }
        }
    }

    extension Kernel.Thread.Executor.Completion {
        public func asUnownedTaskExecutor() -> UnownedTaskExecutor {
            unsafe UnownedTaskExecutor(ordinary: self)
        }
    }

    extension Kernel.Thread.Executor.Completion {

        public var kernel: Kernel.Completion {
            _read { yield _kernel! }
            _modify { yield &_kernel! }
        }
    }

    extension Kernel.Thread.Executor.Completion {

        public func shutdown() {
            _shutdown.set()
            kernelWakeup.wake()
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

    extension Kernel.Thread.Executor.Completion {
        private func runLoop() {
            var eventBuffer: [Kernel.Completion.Event] = []
            eventBuffer.reserveCapacity(maxCompletionsPerPoll)

            while !_shutdown.isSet {
                drainJobs()
                if _shutdown.isSet { break }

                let k = _kernel.take()!
                eventBuffer.removeAll(keepingCapacity: true)
                let waitError: Kernel.Completion.Error?
                do throws(Kernel.Completion.Error) {

                    _ = try k.flush()
                    k.drain { event in
                        eventBuffer.append(event)
                    }
                    if eventBuffer.isEmpty {
                        k.notification?.wait()
                        k.drain { event in
                            eventBuffer.append(event)
                        }
                    }
                    waitError = nil
                } catch {
                    waitError = error
                }
                _kernel = consume k

                if _shutdown.isSet { break }

                let outcome = eventBuffer.withUnsafeBufferPointer { base in
                    unsafe tick {
                        () throws(Kernel.Completion.Error) -> UnsafeBufferPointer<
                            Kernel.Completion.Event
                        > in
                        if let waitError { throw waitError }
                        return unsafe UnsafeBufferPointer<Kernel.Completion.Event>(
                            start: base.baseAddress,
                            count: base.count
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
                    unsafe job.runSynchronously(on: asUnownedSerialExecutor())
                }
            }
        }
    }

#endif

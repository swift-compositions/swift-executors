#if !os(Windows)

    extension Kernel.Thread.Executor {

        @safe
        public final class Polling: SerialExecutor, TaskExecutor, @unsafe @unchecked Sendable {

            let loop: Loop
            private var threadHandle: Kernel.Thread.Handle?

            public init(
                source: consuming Kernel.Event.Source,
                maxEventsPerPoll: Int = 256,
                priorityTracking: Bool = false,
                tick:
                    sending @escaping (
                        () throws(Kernel.Event.Driver.Error) -> UnsafeBufferPointer<Kernel.Event>
                    ) -> Outcome
            ) {
                let loop = Loop(source: consume source, maxEventsPerPoll: maxEventsPerPoll, priorityTracking: priorityTracking, tick: tick)
                self.loop = loop
                self.threadHandle = nil
                loop.executor = self
                self.threadHandle = unsafe Kernel.Thread.trap(
                    Ownership.Transfer.Retained<Kernel.Thread.Executor.Polling.Loop>.Outgoing(loop)
                ) { retained in
                    retained.consume().run()
                }
            }

            /// Ends the thread once its queue is empty, without waiting for it.
            deinit {
                guard let handle = threadHandle.take() else { return }
                loop.end()

                do throws(Kernel.Thread.Error) {
                    try handle.detach()
                } catch {
                }
            }
        }
    }

    extension Kernel.Thread.Executor.Polling {
        public func enqueue(_ job: consuming ExecutorJob) {
            enqueue(UnownedJob(job))
        }

        public func enqueue(_ job: UnownedJob) {

            let runInline: Bool = loop.queueLock.withLock {
                guard !loop.exited else { return true }
                loop.jobs.enqueue(job)
                return false
            }
            if runInline {
                run(job)
            } else {
                loop.waitSource.wakeup.wake()
            }
        }

        public func asUnownedSerialExecutor() -> UnownedSerialExecutor {
            unsafe UnownedSerialExecutor(ordinary: self)
        }

        /// Runs a job here, on this executor.
        func run(_ job: UnownedJob) {
            unsafe Kernel.Thread.Executor.runJob(
                job,
                onSerial: asUnownedSerialExecutor(),
                priorityTracking: loop.priorityTracking
            )
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
            _read { yield loop.waitSource.source }
            _modify { yield &loop.waitSource.source }
        }
    }

    extension Kernel.Thread.Executor.Polling {

        public func shutdown() {
            loop.end()
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

#endif

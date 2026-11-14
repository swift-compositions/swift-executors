#if !os(Windows)

    extension Kernel.Thread.Executor {

        @safe
        public final class Completion: SerialExecutor, TaskExecutor, @unsafe @unchecked Sendable {

            let loop: Loop
            private var threadHandle: Kernel.Thread.Handle?

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
                let loop = Loop(kernel: consume kernel, maxCompletionsPerPoll: maxCompletionsPerPoll, tick: tick)
                self.loop = loop
                self.threadHandle = nil
                loop.executor = self
                self.threadHandle = unsafe Kernel.Thread.trap(
                    Ownership.Transfer.Retained<Kernel.Thread.Executor.Completion.Loop>.Outgoing(loop)
                ) { retained in
                    retained.consume().run()
                }
            }

            /// Ends the thread once its queue is empty, without waiting for it; the kernel is closed when the
            /// loop is let go.
            deinit {
                guard let thread = threadHandle.take() else { return }
                loop.end()

                do throws(Kernel.Thread.Error) {
                    try thread.detach()
                } catch {
                }
            }
        }
    }

    extension Kernel.Thread.Executor.Completion {
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
                unsafe job.runSynchronously(on: asUnownedSerialExecutor())
            } else {
                loop.kernelWakeup.wake()
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

        // swift-format-ignore: NeverForceUnwrap
        public var kernel: Kernel.Completion {
            _read { yield loop.kernel! }
            _modify { yield &loop.kernel! }
        }
    }

    extension Kernel.Thread.Executor.Completion {

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

#if !os(Windows)

    extension Kernel.Thread.Executor.Completion {

        /// What a completion executor's thread runs: its queue of jobs, its kernel and its tick.
        ///
        /// The thread holds the loop and never the executor, so the executor's last reference can go while its
        /// thread waits on the kernel; its `deinit` then sets the flag, wakes the kernel, and the thread ends,
        /// and the loop closes the kernel when it is let go. A job runs only while something holds the executor,
        /// so the loop reaches it through an unowned reference.
        final class Loop {

            var jobs: Executor::Executor.Job.Queue
            var drainBuffer: Executor::Executor.Job.Queue
            let queueLock: Kernel.Thread.Mutex
            var kernel: Kernel.Completion?
            let kernelWakeup: Kernel.Wakeup.Channel
            let shutdown: Executor::Executor.Shutdown.Flag

            var exited: Bool
            let maxCompletionsPerPoll: Int
            let tick:
                (
                    () throws(Kernel.Completion.Error) -> UnsafeBufferPointer<
                        Kernel.Completion.Event
                    >
                ) -> Outcome

            unowned var executor: Kernel.Thread.Executor.Completion?

            init(
                kernel: consuming Kernel.Completion,
                maxCompletionsPerPoll: Int,
                tick:
                    @escaping (
                        () throws(Kernel.Completion.Error) -> UnsafeBufferPointer<
                            Kernel.Completion.Event
                        >
                    ) -> Outcome
            ) {
                self.jobs = .init()
                self.drainBuffer = .init()
                self.queueLock = .init()
                self.kernelWakeup = kernel.wakeup
                self.kernel = consume kernel
                self.shutdown = .init()
                self.exited = false
                self.maxCompletionsPerPoll = maxCompletionsPerPoll
                self.tick = tick
                self.executor = nil
            }

            deinit {
                if let k = kernel.take() {
                    k.close()
                }
            }
        }
    }

    extension Kernel.Thread.Executor.Completion.Loop {

        /// Sets the flag and wakes the kernel, so the thread ends once its queue is empty.
        func end() {
            shutdown.set()
            kernelWakeup.wake()
        }

        func run() {
            var eventBuffer: [Kernel.Completion.Event] = []
            eventBuffer.reserveCapacity(maxCompletionsPerPoll)

            while !shutdown.isSet {
                drainJobs()
                if shutdown.isSet { break }

                guard let k = kernel.take() else {
                    preconditionFailure("Kernel completion storage is non-nil while the run loop is active")
                }
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
                kernel = consume k

                if shutdown.isSet { break }

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
                    shutdown.set()
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

                        exited = true
                    }
                    return drainBuffer.isEmpty
                }
                guard !isEmpty else { return }
                while let job = drainBuffer.dequeue() {
                    guard let executor else { return }
                    unsafe job.runSynchronously(on: executor.asUnownedSerialExecutor())
                }
            }
        }
    }

#endif

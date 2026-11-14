#if !os(Windows)

    extension Kernel.Thread.Executor.Polling {

        /// What a polling executor's thread runs: its queue of jobs, its event source and its tick.
        ///
        /// The thread holds the loop and never the executor, so the executor's last reference can go while its
        /// thread waits on the kernel; its `deinit` then sets the flag, wakes the source, and the thread ends. A
        /// job runs only while something holds the executor, so the loop reaches it through an unowned reference.
        final class Loop {

            var jobs: Executor::Executor.Job.Queue
            var drainBuffer: Executor::Executor.Job.Queue
            let queueLock: Kernel.Thread.Mutex
            var waitSource: Executor::Executor.Wait.Event.Source
            let shutdown: Executor::Executor.Shutdown.Flag

            var exited: Bool
            let maxEventsPerPoll: Int
            let priorityTracking: Bool
            let tick:
                (
                    () throws(Kernel.Event.Driver.Error) -> UnsafeBufferPointer<Kernel.Event>
                ) -> Outcome

            unowned var executor: Kernel.Thread.Executor.Polling?

            init(
                source: consuming Kernel.Event.Source,
                maxEventsPerPoll: Int,
                priorityTracking: Bool,
                tick:
                    @escaping (
                        () throws(Kernel.Event.Driver.Error) -> UnsafeBufferPointer<Kernel.Event>
                    ) -> Outcome
            ) {
                self.jobs = .init()
                self.drainBuffer = .init()
                self.queueLock = .init()
                self.waitSource = .init(source: consume source)
                self.shutdown = .init()
                self.exited = false
                self.maxEventsPerPoll = maxEventsPerPoll
                self.priorityTracking = priorityTracking
                self.tick = tick
                self.executor = nil
            }
        }
    }

    extension Kernel.Thread.Executor.Polling.Loop {

        /// Sets the flag and wakes the source, so the thread ends once its queue is empty.
        func end() {
            shutdown.set()
            waitSource.wakeup.wake()
        }

        func run() {
            var eventBuffer = [Kernel.Event](repeating: Kernel.Event.empty, count: maxEventsPerPoll)
            while !shutdown.isSet {
                drainJobs()
                if shutdown.isSet { break }

                let count: Int
                let waitError: Kernel.Event.Driver.Error?
                do throws(Kernel.Event.Driver.Error) {
                    count = try waitSource.wait(deadline: nil, into: &eventBuffer)
                    waitError = nil
                } catch {
                    count = 0
                    waitError = error
                }
                if shutdown.isSet { break }

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
                    executor.run(job)
                }
            }
        }
    }

#endif

extension Kernel.Thread {

    public final class Executor: SerialExecutor, TaskExecutor, @unsafe @unchecked Sendable {

        let loop: Loop
        private var threadHandle: Kernel.Thread.Handle?

        public init(mode: Mode = .serial, priorityTracking: Bool = false) {
            let loop = Loop(mode: mode, priorityTracking: priorityTracking)
            self.loop = loop
            self.threadHandle = nil
            loop.executor = self
            self.threadHandle = unsafe Kernel.Thread.trap(
                Ownership.Transfer.Retained<Kernel.Thread.Executor.Loop>.Outgoing(loop)
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

extension Kernel.Thread.Executor {
    public func enqueue(_ job: UnownedJob) {

        let runInline: Bool = loop.wait.withLock {
            guard !loop.exited else { return true }
            loop.jobs.enqueue(job)
            return false
        }
        if runInline {
            run(job)
        } else {
            loop.wait.wake()
        }
    }

    public func asUnownedSerialExecutor() -> UnownedSerialExecutor {
        unsafe UnownedSerialExecutor(ordinary: self)
    }

    /// Runs a job here, as this executor's mode says.
    func run(_ job: UnownedJob) {
        switch loop.mode {
        case .serial:
            unsafe Self.runJob(
                job,
                onSerial: asUnownedSerialExecutor(),
                priorityTracking: loop.priorityTracking
            )

        case .task:
            unsafe Self.runJob(
                job,
                onTask: asUnownedTaskExecutor(),
                priorityTracking: loop.priorityTracking
            )
        }
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

    public func shutdown() {
        guard let handle = threadHandle.take() else {
            preconditionFailure(
                "Kernel.Thread.Executor.shutdown() called on already-shutdown or never-started executor"
            )
        }

        loop.end()

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

extension Kernel.Thread.Executor {

    internal static func runJob(
        _ job: UnownedJob,
        onSerial executor: UnownedSerialExecutor,
        priorityTracking: Bool
    ) {
        #if os(macOS) || os(iOS) || os(tvOS) || os(watchOS) || os(visionOS)
            if priorityTracking,
                let qos = Darwin.Kernel.Thread.QoS(priority: UInt32(job.priority.rawValue))
            {
                qos.withOverride {
                    unsafe job.runSynchronously(on: executor)
                }
            } else {
                unsafe job.runSynchronously(on: executor)
            }
        #else
            unsafe job.runSynchronously(on: executor)
        #endif
    }

    internal static func runJob(
        _ job: UnownedJob,
        onTask executor: UnownedTaskExecutor,
        priorityTracking: Bool
    ) {
        #if os(macOS) || os(iOS) || os(tvOS) || os(watchOS) || os(visionOS)
            if priorityTracking,
                let qos = Darwin.Kernel.Thread.QoS(priority: UInt32(job.priority.rawValue))
            {
                qos.withOverride {
                    unsafe job.runSynchronously(on: executor)
                }
            } else {
                unsafe job.runSynchronously(on: executor)
            }
        #else
            unsafe job.runSynchronously(on: executor)
        #endif
    }
}

extension Kernel.Thread.Executor {

    public var runtime: Executor.Runtime {
        loop.wait.withLock {
            if loop.exited { return .terminated }
            if loop.shutdown.isSet { return .draining }
            return .running
        }
    }
}

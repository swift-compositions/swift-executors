//
//  Kernel.Thread.Executor.swift
//  swift-executors
//
//  Created by Coen ten Thije Boonkkamp on 28/12/2025.
//

extension Kernel.Thread {
    /// A serial executor backed by a single dedicated OS thread.
    ///
    /// Conforms to both `SerialExecutor` (for actor pinning via `unownedExecutor`)
    /// and `TaskExecutor` (for `withTaskExecutorPreference`).
    ///
    /// ## Safety Invariant
    ///
    /// This type is `Sendable` by virtue of internal synchronization: the job
    /// queue (`jobs`), the shutdown flag (`_shutdown`), and the stored thread
    /// handle (`threadHandle`) are all mutated exclusively under
    /// `wait: Executor.Wait.Condvar` -- a mutex + condition variable wrapper.
    /// `enqueue`, `runLoop`, and `shutdown` each route their state accesses
    /// through `wait.withLock`, and cross-thread wake-ups go through
    /// `wait.wake()` / `wait.wake.all()`. The caller MUST interact with the
    /// executor only through its public API (`enqueue`, `shutdown`, the
    /// unowned-executor accessors); reaching into the stored state otherwise
    /// is undefined behaviour.
    ///
    /// ## Intended Use
    ///
    /// - Pinning Swift actors to a dedicated OS thread via `unownedExecutor`
    ///   (`.serial` mode).
    /// - Running jobs under `withTaskExecutorPreference` with a task-executor
    ///   identity (`.task` mode).
    /// - Workloads that need deterministic OS-level thread identity (e.g.,
    ///   thread-local state, TLS-backed subsystems, priority pinning).
    ///
    /// ## Non-Goals
    ///
    /// - Not a work-stealing pool. For fan-out across N threads with stealing
    ///   use `Kernel.Thread.Executor.Stealing`.
    /// - Not safe to shutdown from its own thread. Doing so deadlocks -- the
    ///   implementation detects the case and detaches instead of joining.
    /// - Not idempotent on shutdown. `shutdown()` must be called exactly once
    ///   before the executor is deallocated; a second call traps.
    ///
    /// ## Run Identity
    ///
    /// The executor reports the correct identity when running jobs (otherwise
    /// the Swift Concurrency runtime re-enqueues indefinitely):
    /// - `.serial` (default): `runSynchronously(on: serialExecutor)` -- use
    ///   for actor pinning via `unownedExecutor`.
    /// - `.task`: `runSynchronously(on: taskExecutor)` -- use with
    ///   `withTaskExecutorPreference`.
    public final class Executor: SerialExecutor, TaskExecutor, @unsafe @unchecked Sendable {

        private let mode: Mode
        private let priorityTracking: Bool
        private let wait: Executor_Primitives.Executor.Wait.Condvar
        private var jobs: Executor_Primitives.Executor.Job.Queue
        private let _shutdown: Executor_Primitives.Executor.Shutdown.Flag
        private var threadHandle: Kernel.Thread.Handle?

        /// Creates a new executor thread.
        ///
        /// The thread starts immediately and begins waiting for jobs.
        ///
        /// - Parameters:
        ///   - mode: Controls which identity is reported to the runtime.
        ///     Use `.serial` (default) for actor pinning, `.task` for
        ///     `withTaskExecutorPreference`.
        ///   - priorityTracking: If `true`, this thread's QoS class is
        ///     bumped to match each job's priority for the duration of
        ///     job execution on Darwin (no-op on other platforms). See
        ///     `Research/priority-escalation-policy.md`. Default
        ///     `false`.
        public init(mode: Mode = .serial, priorityTracking: Bool = false) {
            self.mode = mode
            self.priorityTracking = priorityTracking
            self.wait = .init()
            self.jobs = .init()
            self._shutdown = .init()

            self.threadHandle = unsafe Kernel.Thread.trap(Ownership.Transfer.Retained<Kernel.Thread.Executor>.Outgoing(self)) { retained in
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
            handle.detach()
        }
    }
}

// MARK: - SerialExecutor

extension Kernel.Thread.Executor {
    public func enqueue(_ job: UnownedJob) {
        let runInline: Bool = wait.withLock {
            guard !_shutdown.isSet else { return true }
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

// MARK: - Isolation Verification

extension Kernel.Thread.Executor {
    /// Verifies the current execution context is on this executor's thread.
    ///
    /// Called by the Swift concurrency runtime when `assumeIsolated` cannot
    /// determine executor identity via task-local state.
    ///
    /// Returns `true` if the calling thread is this executor's OS thread,
    /// `false` if the thread is known but not current, `nil` if the thread
    /// handle is unavailable (post-shutdown).
    public func isIsolatingCurrentContext() -> Bool? {
        threadHandle?.isCurrent
    }

    /// Crash-or-pass isolation check. Called by the runtime as a last
    /// resort after `isIsolatingCurrentContext()` returns `nil`.
    public func checkIsolated() {
        guard isIsolatingCurrentContext() == true else {
            preconditionFailure(
                "Kernel.Thread.Executor: expected current thread to be the executor's thread"
            )
        }
    }
}

// MARK: - TaskExecutor

extension Kernel.Thread.Executor {
    public func enqueue(_ job: consuming ExecutorJob) {
        enqueue(UnownedJob(job))
    }
}

// MARK: - Run Loop

extension Kernel.Thread.Executor {
    fileprivate func runLoop() {
        while true {
            let job: UnownedJob? = wait.withLock {
                while jobs.isEmpty && !_shutdown.isSet {
                    wait.wait()
                }
                guard !_shutdown.isSet || !jobs.isEmpty else { return nil }
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

// MARK: - Shutdown

extension Kernel.Thread.Executor {
    /// Shutdown the executor thread.
    ///
    /// Signals the run loop to exit after processing any remaining jobs,
    /// then joins the thread.
    ///
    /// - Precondition: Must NOT be called from the executor thread itself.
    /// - Precondition: Must be called exactly once before the executor is deallocated.
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
            // Actor deinit dispatched on this executor's own thread.
            // Cannot join — would deadlock. The thread exits promptly
            // because _shutdown is set and the run loop checks it each
            // iteration. Detach releases the handle; the OS reclaims
            // the thread stack when it exits.
            handle.detach()
        } else {
            handle.join()
        }
    }
}

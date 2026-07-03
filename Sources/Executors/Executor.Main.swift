//
//  Executor.Main.swift
//  swift-executors
//

#if os(macOS) || os(iOS) || os(tvOS) || os(watchOS) || os(visionOS)
    import Dispatch
#endif

extension Executor {
    /// Main-thread serial executor.
    ///
    /// On Darwin, delegates to `DispatchQueue.main` for automatic main-thread
    /// integration. On Linux/Windows, provides a condvar-based pump that the
    /// consumer must drive via `run()`.
    ///
    /// ## Safety Invariant
    ///
    /// This type is `Sendable` by virtue of internal synchronization:
    /// - On Darwin, all enqueue paths dispatch into `DispatchQueue.main`,
    ///   which owns its own lock-free MPSC enqueue primitive and is
    ///   architecturally Sendable.
    /// - On Linux / Windows, the job queue (`jobs`), condition variable
    ///   (`wait`), and shutdown flag (`_shutdown`) are mutated exclusively
    ///   under `wait: Executor.Wait.Condvar` -- a mutex + condvar wrapper.
    ///   `enqueue`, `run`, and `shutdown` route state accesses through
    ///   `wait.withLock`.
    ///
    /// The caller MUST interact with the executor only through its public
    /// API. Do not read or mutate the platform-specific stored state
    /// directly.
    ///
    /// ## Intended Use
    ///
    /// - Pinning actors to the OS main thread via `Executor.Main.shared`.
    /// - Providing a SerialExecutor target on platforms without an ambient
    ///   main run loop (Linux, Windows) by manually driving `run()` from
    ///   the main thread.
    ///
    /// ## Non-Goals
    ///
    /// - Not a TaskExecutor. Main-thread dispatch implies serial ordering;
    ///   task-executor semantics are not offered.
    /// - Not a substitute for `DispatchMain()`. On Linux/Windows the pump
    ///   blocks only until `shutdown()`; there is no ambient integration
    ///   with OS run loops.
    /// - Not multi-instance. The type is exposed only via
    ///   `Executor.Main.shared`.
    public final class Main: SerialExecutor, @unsafe @unchecked Sendable {
        #if !(os(macOS) || os(iOS) || os(tvOS) || os(watchOS) || os(visionOS))
            private var jobs: Executor.Job.Queue
            private var drainBuffer: Executor.Job.Queue
            // ⚠️ W5 QUARANTINE (2026-06-12): sympathetic consumer carve — the producer
            // parked Executor Job Priority Primitives (Job.Priority stores Heap<Entry>;
            // heap's umbrella pulls the RED memory-small module; see executor-primitives
            // Package.swift:33). Carved per Ruling 2 / lane-λ in
            // .handoffs/HANDOFF-sockets-restoration-kernel-blocker.md.
            // Restore with heap's round.
            // private var scheduled: Executor.Job.Priority
            private let wait: Executor.Wait.Condvar
            private let _shutdown: Executor.Shutdown.Flag
            private var _stopped: Bool
            private var _isRunning: Bool
        #endif

        private init() {
            #if !(os(macOS) || os(iOS) || os(tvOS) || os(watchOS) || os(visionOS))
                self.jobs = .init()
                self.drainBuffer = .init()
                // self.scheduled = .init()  // W5 QUARANTINE (2026-06-12): Job.Priority parked upstream — restore with heap's round
                self.wait = .init()
                self._shutdown = .init()
                self._stopped = false
                self._isRunning = false
            #endif
        }
    }
}

// MARK: - Shared

extension Executor.Main {
    /// The shared main executor instance.
    public static let shared: Executor.Main = .init()
}

// MARK: - SerialExecutor

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
            wait.withLock { jobs.enqueue(unowned) }
            wait.wake()
        #endif
    }

    public func asUnownedSerialExecutor() -> UnownedSerialExecutor {
        unsafe UnownedSerialExecutor(ordinary: self)
    }
}

// MARK: - Main Loop (Linux/Windows)

#if !(os(macOS) || os(iOS) || os(tvOS) || os(watchOS) || os(visionOS))
    extension Executor.Main {
        /// Drive the main pump on the calling thread.
        ///
        /// Blocks until `shutdown()` or `stop()` is called. Same donation
        /// contract as `Executor.Cooperative` — see that type's docstring.
        ///
        /// - Important: Must be called from the main thread.
        public func run() {
            runUntil { false }
        }

        /// Drive the main pump until a condition is satisfied.
        ///
        /// Same snapshot-then-check drain as `Executor.Cooperative.runUntil`.
        ///
        /// - Precondition: Must not be called while another `run()` or
        ///   `runUntil` is active (re-entrancy prohibited).
        public func runUntil(_ condition: () -> Bool) {
            precondition(!_isRunning, "nested runUntil is not supported")
            _isRunning = true
            wait.withLock { _stopped = false }
            defer { _isRunning = false }

            while !_shutdown.isSet {
                if condition() { return }

                let shouldExit = wait.withLock { () -> Bool in
                    // W5 QUARANTINE (2026-06-12): Job.Priority parked upstream — scheduled-drain +
                    // deadline-wait carved; with `enqueue(_:after:)` carved the queue was always
                    // empty, so the bare wait below was already the only live path. Restore with
                    // heap's round.
                    // scheduled.drain(now: Clock.Continuous.now) { jobs.enqueue($0) }

                    while jobs.isEmpty && !_shutdown.isSet && !_stopped {
                        // if let nextDeadline = scheduled.peek {
                        //     let remaining = Clock.Continuous.now.duration(to: nextDeadline)
                        //     if remaining <= .zero {
                        //         scheduled.drain(now: Clock.Continuous.now) { jobs.enqueue($0) }
                        //         continue
                        //     }
                        //     _ = wait.wait(timeout: remaining)
                        //     scheduled.drain(now: Clock.Continuous.now) { jobs.enqueue($0) }
                        // } else {
                        wait.wait()
                        // }
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

        /// Signal the innermost `run()` or `runUntil` to return.
        ///
        /// Non-destructive: the executor remains usable after `stop()`.
        public func stop() {
            wait.withLock { _stopped = true }
            wait.wake.all()
        }

        // ⚠️ W5 QUARANTINE (2026-06-12): sympathetic consumer carve — the producer
        // parked Executor Job Priority Primitives (Job.Priority stores Heap<Entry>;
        // heap's umbrella pulls the RED memory-small module; see executor-primitives
        // Package.swift:33). Carved per Ruling 2 / lane-λ in
        // .handoffs/HANDOFF-sockets-restoration-kernel-blocker.md.
        // Restore with heap's round.

        // /// Schedule a job for execution at a future deadline.
        // public func enqueue(
        //     _ job: consuming ExecutorJob,
        //     after delay: Duration
        // ) {
        //     let deadline = Clock.Continuous.now.advanced(by: delay)
        //     let unowned = UnownedJob(job)
        //     wait.withLock { scheduled.schedule(unowned, at: deadline) }
        //     wait.wake()
        // }

        /// Signal the main pump to exit permanently.
        public func shutdown() {
            _shutdown.set()
            wait.wake.all()
        }
    }
#endif

//
//  Executor.Cooperative.swift
//  swift-executors
//

extension Executor {
    /// Runs on the caller's thread; no OS thread spawned.
    ///
    /// Caller drives the run loop explicitly via `run()`. Closest analogs:
    /// Tokio `current_thread`, futures-rs `LocalPool`, Apple `CooperativeExecutor`.
    ///
    /// ## Safety Invariant
    ///
    /// This type is `Sendable` by virtue of internal synchronization: the
    /// job queue (`jobs`), condition variable (`wait`), and shutdown flag
    /// (`_shutdown`) are mutated exclusively under `wait: Executor.Wait.Condvar`
    /// -- a mutex + condvar wrapper. `enqueue`, `run`, and `shutdown` route
    /// state accesses through `wait.withLock`. The caller MUST interact with
    /// the executor only through its public API; touching the stored state
    /// directly is undefined behaviour.
    ///
    /// ## Intended Use
    ///
    /// - Single-threaded cooperative task execution on the caller's thread
    ///   (e.g., test harnesses, deterministic simulation, REPL drivers).
    /// - Unit tests that need a drain-to-completion executor without
    ///   spawning an OS thread.
    ///
    /// ## Donation Contract
    ///
    /// - **Yield policy:** Snapshot-then-check. Each iteration drains a
    ///   snapshot of pending jobs, then re-checks the exit condition.
    ///   Prevents the infinite-drain bug the stdlib hit and fixed
    ///   (`0fbd382e9ca`, `bd27a14ea00`).
    /// - **Revocation:** `runUntil(_:)` accepts a condition callback;
    ///   `stop()` signals the innermost invocation to return.
    ///   Revocation latency is bounded by the longest single job
    ///   execution, not by sleep duration (condvar wakes on enqueue).
    /// - **Completion guarantee:** Always. `run()` returns on `shutdown()`
    ///   or `stop()`. `runUntil(_:)` returns when the condition is
    ///   satisfied, or `stop()`/`shutdown()` is called.
    /// - **Priority:** Caller-owned. The donated thread runs at the OS
    ///   priority of the donating context. The executor does not adjust it.
    ///   FIFO drain order for v1.
    /// - **Re-entrancy:** Prohibited. Nested `runUntil` / `run()` traps.
    /// - **`stop()` vs `shutdown()`:** `shutdown()` dominates. If both are
    ///   in flight, the executor exits permanently. `stop()` alone halts
    ///   the innermost `runUntil`; `shutdown()` halts everything and is
    ///   irreversible.
    ///
    /// ## Non-Goals
    ///
    /// - Not a TaskExecutor. Cooperative scheduling implies serial actor
    ///   identity; task-executor semantics are not offered.
    /// - Not multi-thread. Enqueues from other threads are allowed, but
    ///   execution is always on the `run()` / `runUntil` caller.
    ///
    /// ## Usage
    /// ```swift
    /// let executor = Executor.Cooperative()
    /// // From another task:
    /// executor.enqueue(job)
    /// // On the calling thread:
    /// executor.run()              // blocks until shutdown() or stop()
    /// executor.runUntil { done }  // blocks until done or stop()/shutdown()
    /// ```
    public final class Cooperative: SerialExecutor, @unsafe @unchecked Sendable {
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
        /// Lock-protected by `wait`. Written by `stop()`, reset at `runUntil` entry.
        private var _stopped: Bool
        /// Single-thread only (donated thread). Re-entrancy guard.
        private var _isRunning: Bool

        public init() {
            self.jobs = .init()
            self.drainBuffer = .init()
            // self.scheduled = .init()  // W5 QUARANTINE (2026-06-12): Job.Priority parked upstream — restore with heap's round
            self.wait = .init()
            self._shutdown = .init()
            self._stopped = false
            self._isRunning = false
        }
    }
}

// MARK: - SerialExecutor

extension Executor.Cooperative {
    public func enqueue(_ job: consuming ExecutorJob) {
        enqueue(UnownedJob(job))
    }

    public func enqueue(_ job: UnownedJob) {
        let accepted: Bool = wait.withLock {
            guard !_shutdown.isSet else { return false }
            jobs.enqueue(job)
            return true
        }
        if accepted {
            wait.wake()
            return
        }
        // Cooperative variant of the `Kernel.Thread.Executor` Teardown
        // Contract: after `shutdown()` no thread can ever run this job --
        // `run()`/`runUntil` never execute again, and running inline on
        // the enqueuing thread would violate the donation contract (all
        // execution happens on the donated thread). The job is abandoned:
        // its continuation never resumes. Loud in debug builds; the
        // documented silent drop in release.
        assertionFailure(
            "Executor.Cooperative.enqueue(_:) after shutdown(): job abandoned — its continuation will never resume"
        )
    }

    public func asUnownedSerialExecutor() -> UnownedSerialExecutor {
        unsafe UnownedSerialExecutor(ordinary: self)
    }
}

// MARK: - Scheduled Enqueue

// ⚠️ W5 QUARANTINE (2026-06-12): sympathetic consumer carve — the producer
// parked Executor Job Priority Primitives (Job.Priority stores Heap<Entry>;
// heap's umbrella pulls the RED memory-small module; see executor-primitives
// Package.swift:33). Carved per Ruling 2 / lane-λ in
// .handoffs/HANDOFF-sockets-restoration-kernel-blocker.md.
// Restore with heap's round.

// extension Executor.Cooperative {
//     /// Schedule a job for execution at a future deadline.
//     ///
//     /// The job is placed into the internal priority queue. The donated
//     /// thread's drain loop wakes on the deadline via timed condvar wait,
//     /// then moves the job to the immediate queue for execution.
//     ///
//     /// When `SchedulingExecutor` ships in the SDK (absent as of macOS
//     /// 26.4 — protocol exists in stdlib source but not in the
//     /// `.swiftinterface`), this method's signature matches the protocol
//     /// requirement; conformance is a one-line addition.
//     public func enqueue(
//         _ job: consuming ExecutorJob,
//         after delay: Duration
//     ) {
//         let deadline = Clock.Continuous.now.advanced(by: delay)
//         let unowned = UnownedJob(job)
//         wait.withLock { scheduled.schedule(unowned, at: deadline) }
//         wait.wake()
//     }
// }

// MARK: - Run Loop

extension Executor.Cooperative {
    /// Drive the run loop on the caller's thread.
    ///
    /// Blocks until `shutdown()` or `stop()` is called. Equivalent to
    /// `runUntil { false }`.
    public func run() {
        runUntil { false }
    }

    /// Drive the run loop until a condition is satisfied.
    ///
    /// Uses snapshot-then-check: each iteration takes a snapshot of
    /// pending jobs, drains the snapshot, then re-checks `condition`.
    /// Returns when `condition()` returns `true`, `stop()` is called,
    /// or `shutdown()` is called.
    ///
    /// - Precondition: Must not be called while another `run()` or
    ///   `runUntil` is active on this executor (re-entrancy prohibited).
    /// - Parameter condition: Checked after each drain snapshot. Return
    ///   `true` to exit.
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
                // // Move ready scheduled jobs into the immediate queue
                // scheduled.drain(now: Clock.Continuous.now) { jobs.enqueue($0) }

                // Wait until: immediate jobs available, next deadline fires,
                // stopped, or shutdown
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
    /// Revocation latency is bounded by the longest single job execution
    /// (condvar wakes immediately, unlike the stdlib's nanosleep).
    ///
    /// Safe to call from any thread, including from within a job
    /// executing on the donated thread.
    public func stop() {
        wait.withLock { _stopped = true }
        wait.wake.all()
    }

    /// Signal the run loop to exit permanently.
    ///
    /// Irreversible. Dominates `stop()` — if both are in flight, the
    /// executor exits permanently. Jobs enqueued after shutdown begins
    /// are abandoned: they are dropped without ever executing, so any
    /// continuation awaiting them never resumes. Debug builds assert on
    /// such an enqueue; release builds drop silently (the Cooperative
    /// variant of the `Kernel.Thread.Executor` Teardown Contract — there
    /// is no executor-owned thread left to drain to, and running inline
    /// on the enqueuing thread would violate the donation contract).
    public func shutdown() {
        _shutdown.set()
        wait.wake.all()
    }
}

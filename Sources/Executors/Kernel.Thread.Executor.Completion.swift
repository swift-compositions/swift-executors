//
//  Kernel.Thread.Executor.Completion.swift
//  swift-executors
//

// WHY: #if !os(Windows) — Kernel.Completion is backed by io_uring on
// Linux today; Darwin users can inject a custom Kernel.Completion (for
// testing or future backends), so the type compiles there as well. On
// Windows the ecosystem plans a separate Kernel.Thread.Executor.IOCP
// sibling per executor-package-design.md Decision #6.
#if !os(Windows)

    extension Kernel.Thread.Executor {
        /// Single-thread executor whose wait primitive is a kernel completion
        /// resource.
        ///
        /// One OS thread, one job queue, one owned `Kernel.Completion`. The run
        /// loop interleaves drain-jobs with the proactor phase cycle
        /// (flush → drain → maybe-wait → drain) on the completion resource,
        /// then delivers the drained events to a consumer-supplied tick body.
        /// Tick receives a `wait` thunk that either returns the buffer of
        /// completion events from the cycle or throws the kernel error.
        ///
        /// ## Event Flow
        ///
        /// ```
        /// drain jobs
        ///   → flush pending submissions
        ///   → drain completion queue
        ///   → if empty: block on kernel notification
        ///   → drain again
        ///   → tick(wait: try or catch)
        ///   → repeat
        /// ```
        ///
        /// The consumer never sees the phase ordering. Submissions made by
        /// actor methods during `drain jobs` (via `executor.kernel.submit(…)`)
        /// are guaranteed to be flushed before any blocking wait — the
        /// flush-before-wait constraint of completion-based I/O is encoded in
        /// the run loop and cannot be violated by the consumer.
        ///
        /// ## Backend Neutrality
        ///
        /// This executor references only `Kernel.Completion`,
        /// `Kernel.Completion.Error`, and `Kernel.Completion.Event`. It names
        /// no platform backend (io_uring, IOCP, or otherwise). Any future
        /// backend that fits the `Kernel.Completion` contract is served
        /// without modification.
        ///
        /// ## Error Policy
        ///
        /// The executor does not classify kernel errors. Every outcome —
        /// success or failure — is delivered via the typed-throws `wait`
        /// thunk. Tick catches with `throws(Kernel.Completion.Error)` and
        /// decides whether to `.continue` (optionally yielding first) or
        /// `.halt`. Consumers that need transient-error retry implement it in
        /// tick; consumers that treat any error as fatal return `.halt`
        /// directly.
        ///
        /// ## Race Safety
        ///
        /// The tick body runs on the executor's own thread — the same thread
        /// that dispatches actor jobs. Domain state touched by tick is
        /// single-threaded.
        ///
        /// ## Teardown Contract
        ///
        /// Applies the `Kernel.Thread.Executor` Teardown Contract: `enqueue`
        /// keys inline-vs-queue on `_loopExited`, not `_shutdown`. The
        /// terminal `drainJobs()` call (made once, after the run loop's
        /// `while !_shutdown.isSet` breaks) publishes `_loopExited` in the
        /// SAME `queueLock` critical section as the empty-queue observation
        /// that ends it, so a job enqueued while that terminal drain is
        /// still finding work is guaranteed to be picked up by it, and one
        /// enqueued after it has verifiably finished draining runs inline
        /// with no executor-thread concurrency possible.
        ///
        /// ## Safety Invariant
        ///
        /// This type is `Sendable` by virtue of internal synchronization.
        /// Cross-thread mutable state is guarded as follows:
        /// - `jobs` / `drainBuffer` : protected by `queueLock: Kernel.Thread.Mutex`.
        ///   Every `enqueue` / `drainJobs` operation serializes through
        ///   `queueLock.withLock`.
        /// - `_shutdown` : atomic `Shutdown.Flag`.
        /// - `_loopExited` : `Bool`, guarded by `queueLock`; published only in
        ///   the terminal `drainJobs()` call's critical section. See
        ///   ``Teardown Contract``.
        /// - `_kernel` : the `~Copyable` completion resource is thread-confined
        ///   to the executor's OS thread; it is taken out of its `Optional`
        ///   slot for the duration of each iteration's I/O phase and
        ///   restored before tick runs. Cross-thread wakeup is delivered via
        ///   the separately-held `kernelWakeup: Kernel.Wakeup.Channel`
        ///   (`Sendable` by construction).
        /// - `threadHandle` : mutated only at construction and shutdown
        ///   boundaries.
        ///
        /// The caller MUST interact with the executor only through the public
        /// API (`enqueue`, `shutdown`, the unowned-executor accessors, the
        /// `kernel` coroutine-scoped accessor); reaching into stored state
        /// otherwise is undefined behaviour.
        ///
        /// ## Intended Use
        ///
        /// - Completion-based I/O executors where actor jobs, kernel
        ///   submissions, and kernel completions are all multiplexed on the
        ///   same thread (e.g., `io_uring`-driven I/O on Linux).
        /// - Foundation-layer proactor threads that serve a single owner
        ///   actor.
        ///
        /// ## Non-Goals
        ///
        /// - Not a Windows executor (see `#if !os(Windows)` guard). A future
        ///   `Kernel.Thread.Executor.IOCP` sibling will serve that role.
        /// - Not a work-stealing executor. Single-threaded by design.
        ///
        /// ## Lifecycle
        /// Call `shutdown()` before deallocation.
        @safe
        public final class Completion: SerialExecutor, TaskExecutor, @unsafe @unchecked Sendable {

            private var jobs: Executor_Primitives.Executor.Job.Queue
            private var drainBuffer: Executor_Primitives.Executor.Job.Queue
            private let queueLock: Kernel.Thread.Mutex
            private var _kernel: Kernel.Completion?
            private let kernelWakeup: Kernel.Wakeup.Channel
            private let _shutdown: Executor_Primitives.Executor.Shutdown.Flag
            /// `true` once the terminal `drainJobs()` call (made after the
            /// run loop's `while` breaks) has observed the queue empty.
            /// Guarded by `queueLock`; published in that same critical
            /// section. See ``Teardown Contract``.
            private var _loopExited: Bool
            private var threadHandle: Kernel.Thread.Handle?
            private let maxCompletionsPerPoll: Int
            private let tick:
                (
                    () throws(Kernel.Completion.Error) -> UnsafeBufferPointer<
                        Kernel.Completion.Event
                    >
                ) -> Outcome

            /// Creates a completion executor.
            ///
            /// Spawns an OS thread that runs the event loop. The run loop
            /// drains pending actor jobs, runs the proactor phase cycle
            /// (flush → drain → maybe-wait → drain) against the kernel
            /// completion resource, then invokes `tick` with a typed-throws
            /// `wait` thunk carrying the drained events or the kernel error.
            ///
            /// - Parameters:
            ///   - kernel: The kernel completion resource to drive. Consumed.
            ///   - maxCompletionsPerPoll: Initial capacity of the events
            ///     buffer the drain visitor populates per cycle. Default 256.
            ///   - tick: Called each iteration with a `wait` thunk. Invoke
            ///     `try wait()` to either receive the events drained this
            ///     cycle or propagate the kernel error via
            ///     `Kernel.Completion.Error`. Returns `.continue` to keep
            ///     running or `.halt` to stop. Runs on the executor's own
            ///     thread. The buffer pointer returned by `wait()` is valid
            ///     only for the duration of the tick call. Tick MUST call
            ///     `wait()` — if it doesn't, the cycle's events or error
            ///     are dropped.
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
                // Emergency: shutdown was never called. Halt and detach.
                if let thread = threadHandle.take() {
                    _shutdown.set()
                    kernelWakeup.wake()
                    // Best-effort detach: deinit cannot propagate a typed
                    // error, and a detach failure here is non-actionable —
                    // this is emergency cleanup for a caller that never
                    // called shutdown(), and the handle is unreachable
                    // either way once this scope exits.
                    do throws(Kernel.Thread.Error) {
                        try thread.detach()
                    } catch {
                    }
                }
                // Consume and close the kernel. Single close point.
                if let k = _kernel.take() {
                    k.close()
                }
            }
        }
    }

    // MARK: - SerialExecutor

    extension Kernel.Thread.Executor.Completion {
        public func enqueue(_ job: consuming ExecutorJob) {
            enqueue(UnownedJob(job))
        }

        public func enqueue(_ job: UnownedJob) {
            // Teardown Contract (see the type's documentation): queue
            // whenever the terminal drain has not yet verifiably exited --
            // even mid-shutdown, the executor thread still executes queued
            // jobs. Run inline only once the loop has verifiably exited.
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

    // MARK: - Isolation Verification

    extension Kernel.Thread.Executor.Completion {
        /// Verifies the current execution context is on this executor's thread.
        ///
        /// Called by the Swift concurrency runtime when `assumeIsolated`
        /// cannot determine executor identity via task-local state (e.g.,
        /// synchronous callbacks from the run loop's tick closure that
        /// execute outside a Swift Task context).
        public func isIsolatingCurrentContext() -> Bool? {
            threadHandle?.isCurrent
        }

        /// Crash-or-pass isolation check. Called by the runtime as a last
        /// resort after `isIsolatingCurrentContext()` returns `nil`.
        public func checkIsolated() {
            guard isIsolatingCurrentContext() == true else {
                preconditionFailure(
                    "Kernel.Thread.Executor.Completion: expected current thread to be the executor's thread"
                )
            }
        }
    }

    // MARK: - TaskExecutor

    extension Kernel.Thread.Executor.Completion {
        public func asUnownedTaskExecutor() -> UnownedTaskExecutor {
            unsafe UnownedTaskExecutor(ordinary: self)
        }
    }

    // MARK: - Kernel Access

    extension Kernel.Thread.Executor.Completion {
        /// Direct access to the underlying completion resource for
        /// submit/cancel from actor methods. Coroutine-scoped — the
        /// reference cannot escape.
        ///
        /// MUST be called from the executor's own thread (actor methods
        /// pinned to this executor). Single-threaded access is guaranteed
        /// by actor isolation, not by this accessor.
        public var kernel: Kernel.Completion {
            _read { yield _kernel! }
            _modify { yield &_kernel! }
        }
    }

    // MARK: - Shutdown

    extension Kernel.Thread.Executor.Completion {
        /// Signal the run loop to halt and clean up the thread.
        ///
        /// Safe to call from any thread, including the executor's own thread.
        /// When called from the executor's own thread (e.g., actor deinit
        /// dispatched on this executor), the thread is detached instead of
        /// joined — the thread exits promptly because `_shutdown` is set and
        /// any `[weak self]` tick closure returns `.halt`.
        public func shutdown() {
            _shutdown.set()
            kernelWakeup.wake()
            if let handle = threadHandle.take() {
                if handle.isCurrent {
                    // Best-effort detach: a failure here is non-actionable
                    // at teardown — the executor thread still runs to
                    // completion on its own once `_shutdown` is observed.
                    do throws(Kernel.Thread.Error) {
                        try handle.detach()
                    } catch {
                    }
                } else {
                    // Best-effort join: a failure here is non-actionable —
                    // the executor thread has still run to completion by
                    // the time `pthread_join` returns any error other
                    // than success; there is nothing actionable to do
                    // with the failure at teardown.
                    do throws(Kernel.Thread.Error) {
                        try handle.join()
                    } catch {
                    }
                }
            }
        }
    }

    // MARK: - Run Loop

    extension Kernel.Thread.Executor.Completion {
        private func runLoop() {
            var eventBuffer: [Kernel.Completion.Event] = []
            eventBuffer.reserveCapacity(maxCompletionsPerPoll)

            while !_shutdown.isSet {
                drainJobs()
                if _shutdown.isSet { break }

                // Take-restore pattern: Optional<~Copyable> stored on a class
                // cannot be force-unwrapped for borrow across chained calls.
                // Move the kernel out for the iteration, run the proactor
                // phase cycle against the owned value, then put it back. The
                // run loop is single-threaded so no concurrent access occurs
                // while the slot is empty.
                var k = _kernel.take()!
                eventBuffer.removeAll(keepingCapacity: true)
                let waitError: Kernel.Completion.Error?
                do throws(Kernel.Completion.Error) {
                    // Phase: flush pending submissions to the kernel.
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

                // Materialise the drained buffer as a Sendable-safe local
                // before crossing the tick boundary, per [IMPL-091].
                let outcome = unsafe eventBuffer.withUnsafeBufferPointer { base in
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
            // Terminal drain: this is the LAST time this thread will ever
            // execute a job (runLoop returns right after). Publish
            // `_loopExited` in the same critical section as the empty-queue
            // observation that ends it -- see the Teardown Contract.
            drainJobs(publishLoopExitedIfEmpty: true)
        }

        private func drainJobs(publishLoopExitedIfEmpty: Bool = false) {
            while true {
                let isEmpty: Bool = queueLock.withLock {
                    jobs.drain(into: &drainBuffer)
                    if publishLoopExitedIfEmpty && drainBuffer.isEmpty {
                        // Teardown Contract: an enqueue serialized before
                        // this critical section left its job in `jobs`
                        // (found non-empty, another iteration runs it); one
                        // serialized after observes `_loopExited` and runs
                        // inline once this function (and runLoop) returns.
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

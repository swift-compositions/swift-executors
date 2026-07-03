//
//  Kernel.Thread.Executor.Polling.swift
//  swift-executors
//

// WHY: #if !os(Windows) — Kernel.Event.Source requires epoll (Linux) or
// kqueue (Darwin), neither available on Windows. A future
// Kernel.Thread.Executor.IOCP sibling will serve the Windows role.
// TRACKING: executor-package-design.md Decision #6.
#if !os(Windows)

    extension Kernel.Thread.Executor {
        /// Single-thread executor whose wait primitive is a kernel event source.
        ///
        /// One OS thread, one job queue, one `Executor.Wait.Event.Source`. The run
        /// loop interleaves drain-jobs with a blocking poll on the event source,
        /// then delivers the poll outcome to a consumer-supplied tick body. Tick
        /// receives a `wait` thunk that either returns the events from the cycle
        /// or throws the driver error.
        ///
        /// ## Event Flow
        ///
        /// ```
        /// drain jobs → wait → tick(wait: try or catch) → repeat
        /// ```
        ///
        /// The run loop blocks in `waitSource.wait()` until kernel events arrive
        /// or the wakeup channel fires (from `enqueue()`). Domain-specific event
        /// dispatch and error policy both live in the tick body — errors are
        /// NOT silently retried by the executor.
        ///
        /// ## Error Policy
        ///
        /// The executor does not classify driver errors (EINTR, ENOMEM, EAGAIN,
        /// or fatal). Every outcome — success or failure — is delivered via the
        /// typed-throws `wait` thunk. Tick catches with `throws(Kernel.Event.Driver.Error)`
        /// and decides whether to `.continue` (optionally yielding first) or
        /// `.halt`. Consumers that need transient-error retry implement it in
        /// tick; consumers that treat any error as fatal return `.halt` directly.
        ///
        /// ## Race Safety
        ///
        /// The tick body runs on the executor's own thread — the same thread that
        /// dispatches actor jobs. Domain state touched by tick is single-threaded.
        /// See research doc V5.
        ///
        /// ## Safety Invariant
        ///
        /// This type is `Sendable` by virtue of internal synchronization. Cross-
        /// thread mutable state is guarded as follows:
        /// - `jobs` / `drainBuffer` : protected by `queueLock: Kernel.Thread.Mutex`.
        ///   Every `enqueue` / `drainJobs` operation serializes through
        ///   `queueLock.withLock`.
        /// - `_shutdown` : atomic `Shutdown.Flag`.
        /// - `waitSource` : the kernel event source's wakeup channel is MPSC-safe
        ///   by construction (POSIX `eventfd` / kqueue-signal equivalents); reads
        ///   of the event buffer happen exclusively on the executor's own thread
        ///   inside `runLoop`.
        /// - `threadHandle` : mutated only at construction and shutdown boundaries.
        ///
        /// The `tick` closure fires on the executor's own thread -- the same
        /// thread that dispatches actor jobs -- so domain state touched by `tick`
        /// is single-threaded w.r.t. that executor's actor jobs.
        ///
        /// The caller MUST interact with the executor only through the public
        /// API (`enqueue`, `shutdown`, the unowned-executor accessors, the
        /// `source` coroutine-scoped accessor); reaching into stored state
        /// otherwise is undefined behaviour.
        ///
        /// ## Intended Use
        ///
        /// - Event-loop executors where actor jobs and kernel events must be
        ///   interleaved on the same thread (e.g., epoll/kqueue-driven I/O).
        /// - Foundation-layer reactor threads that multiplex timers, descriptor
        ///   readiness, and actor work on one OS thread.
        ///
        /// ## Non-Goals
        ///
        /// - Not a Windows executor. Depends on `Kernel.Event.Source` which
        ///   requires epoll (Linux) or kqueue (Darwin). A future
        ///   `Kernel.Thread.Executor.IOCP` sibling will serve the Windows role.
        /// - Not idempotent on shutdown -- safe to call from any thread
        ///   (including the executor's own thread), but not from inside the
        ///   `tick` callback at the same moment.
        /// - Not a work-stealing executor. Single-threaded by design.
        ///
        /// ## Lifecycle
        /// Call `shutdown()` before deallocation.
        @safe
        public final class Polling: SerialExecutor, TaskExecutor, @unsafe @unchecked Sendable {

            private var jobs: Executor_Primitives.Executor.Job.Queue
            private var drainBuffer: Executor_Primitives.Executor.Job.Queue
            private let queueLock: Kernel.Thread.Mutex
            private var waitSource: Executor_Primitives.Executor.Wait.Event.Source
            private let _shutdown: Executor_Primitives.Executor.Shutdown.Flag
            private var threadHandle: Kernel.Thread.Handle?
            private let maxEventsPerPoll: Int
            private let priorityTracking: Bool
            private let tick:
                (
                    () throws(Kernel.Event.Driver.Error) -> UnsafeBufferPointer<Kernel.Event>
                ) -> Outcome

            /// Creates a polling executor.
            ///
            /// Spawns an OS thread that runs the event loop. The run loop blocks
            /// in the event source's wait until events arrive or the wakeup
            /// channel fires, then invokes `tick` with a typed-throws `wait`
            /// thunk carrying the poll outcome.
            ///
            /// - Parameters:
            ///   - source: The kernel event source to poll. Consumed.
            ///   - maxEventsPerPoll: Maximum events per poll cycle. Default 256.
            ///   - priorityTracking: If `true`, this executor's thread has its
            ///     QoS class bumped to match each job's priority for the
            ///     duration of job execution on Darwin (no-op on other platforms).
            ///     See `Research/priority-escalation-policy.md`. Default `false`.
            ///   - tick: Called each iteration with a `wait` thunk. Invoke `try wait()`
            ///     to either receive the events from the current cycle or propagate
            ///     the driver error via `Kernel.Event.Driver.Error`. Returns
            ///     `.continue` to keep running or `.halt` to stop. Runs on the
            ///     executor's own thread. The buffer pointer returned by `wait()`
            ///     is valid only for the duration of the tick call. Tick MUST call
            ///     `wait()` — if it doesn't, the cycle's events or error are dropped.
            public init(
                source: consuming Kernel.Event.Source,
                maxEventsPerPoll: Int = 256,
                priorityTracking: Bool = false,
                tick:
                    sending @escaping (
                        () throws(Kernel.Event.Driver.Error) -> UnsafeBufferPointer<Kernel.Event>
                    ) -> Outcome
            ) {
                self.jobs = .init()
                self.drainBuffer = .init()
                self.queueLock = .init()
                self.waitSource = .init(source: consume source)
                self._shutdown = .init()
                self.maxEventsPerPoll = maxEventsPerPoll
                self.priorityTracking = priorityTracking
                unsafe (self.tick = tick)
                self.threadHandle = unsafe Kernel.Thread.trap(Ownership.Transfer.Retained<Kernel.Thread.Executor.Polling>.Outgoing(self)) { retained in
                    retained.consume().runLoop()
                }
            }
        }
    }

    // MARK: - SerialExecutor

    extension Kernel.Thread.Executor.Polling {
        public func enqueue(_ job: consuming ExecutorJob) {
            enqueue(UnownedJob(job))
        }

        public func enqueue(_ job: UnownedJob) {
            let runInline: Bool = queueLock.withLock {
                guard !_shutdown.isSet else { return true }
                jobs.enqueue(job)
                return false
            }
            if runInline {
                unsafe Kernel.Thread.Executor.runJob(
                    job,
                    onSerial: asUnownedSerialExecutor(),
                    priorityTracking: priorityTracking
                )
            } else {
                waitSource.wakeup.wake()
            }
        }

        public func asUnownedSerialExecutor() -> UnownedSerialExecutor {
            unsafe UnownedSerialExecutor(ordinary: self)
        }
    }

    // MARK: - Isolation Verification

    extension Kernel.Thread.Executor.Polling {
        /// Verifies the current execution context is on this executor's thread.
        ///
        /// Called by the Swift concurrency runtime when `assumeIsolated` cannot
        /// determine executor identity via task-local state (e.g., synchronous
        /// callbacks from the run loop's tick closure that execute outside a
        /// Swift Task context).
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
                    "Kernel.Thread.Executor.Polling: expected current thread to be the executor's thread"
                )
            }
        }
    }

    // MARK: - TaskExecutor

    extension Kernel.Thread.Executor.Polling {
        public func asUnownedTaskExecutor() -> UnownedTaskExecutor {
            unsafe UnownedTaskExecutor(ordinary: self)
        }
    }

    // MARK: - Source Access

    extension Kernel.Thread.Executor.Polling {
        /// Direct access to the underlying event source for registration
        /// and configuration. Coroutine-scoped — the reference cannot escape.
        ///
        /// MUST be called from the executor's own thread (actor methods
        /// pinned to this executor). Single-threaded access is guaranteed
        /// by actor isolation, not by this accessor.
        public var source: Kernel.Event.Source {
            _read { yield waitSource.source }
            _modify { yield &waitSource.source }
        }
    }

    // MARK: - Shutdown

    extension Kernel.Thread.Executor.Polling {
        /// Signal the run loop to halt and clean up the thread.
        ///
        /// Safe to call from any thread, including the executor's own thread.
        /// When called from the executor's own thread (e.g., actor deinit
        /// dispatched on this executor), the thread is detached instead of
        /// joined — the thread exits promptly because `_shutdown` is set
        /// and any `[weak self]` tick closure returns `.halt`.
        public func shutdown() {
            _shutdown.set()
            waitSource.wakeup.wake()
            if let handle = threadHandle.take() {
                if handle.isCurrent {
                    handle.detach()
                } else {
                    handle.join()
                }
            }
        }
    }

    // MARK: - Run Loop

    extension Kernel.Thread.Executor.Polling {
        private func runLoop() {
            var eventBuffer = [Kernel.Event](repeating: Kernel.Event.empty, count: maxEventsPerPoll)
            while !_shutdown.isSet {
                drainJobs()
                if _shutdown.isSet { break }

                let count: Int
                let waitError: Kernel.Event.Driver.Error?
                do throws(Kernel.Event.Driver.Error) {
                    count = try waitSource.wait(deadline: nil, into: &eventBuffer)
                    waitError = nil
                } catch {
                    count = 0
                    waitError = error
                }
                if _shutdown.isSet { break }

                let outcome = unsafe eventBuffer.withUnsafeBufferPointer { base in
                    unsafe tick { () throws(Kernel.Event.Driver.Error) -> UnsafeBufferPointer<Kernel.Event> in
                        if let waitError { throw waitError }
                        return unsafe UnsafeBufferPointer<Kernel.Event>(
                            start: base.baseAddress,
                            count: count
                        )
                    }
                }
                if case .halt = outcome {
                    _shutdown.set()
                    break
                }
            }
            drainJobs()
        }

        private func drainJobs() {
            while true {
                queueLock.withLock { jobs.drain(into: &drainBuffer) }
                guard !drainBuffer.isEmpty else { return }
                while let job = drainBuffer.dequeue() {
                    unsafe Kernel.Thread.Executor.runJob(
                        job,
                        onSerial: asUnownedSerialExecutor(),
                        priorityTracking: priorityTracking
                    )
                }
            }
        }
    }

#endif

//
//  Kernel.Thread.Executor.Completion Tests.swift
//  swift-executors
//

import Kernel_Test_Support
import Testing

@testable import Executors

// Behavioral coverage of a REAL completion-backed event loop (io_uring on
// Linux) is platform work outside this package's Darwin test lane. The
// fable-448 rev-1 tests below exercise only the executor's enqueue/shutdown
// teardown contract, using a fully synthetic `Kernel.Completion` (public
// closure-based `Driver` init, nil notification) -- no platform kernel
// resources. This is exactly the injection seam the type's own docs
// advertise ("Darwin users can inject a custom Kernel.Completion for
// testing").

#if !os(Windows)

    extension Kernel.Thread.Executor.Completion {
        enum Test {
            @Suite struct Unit {}
            @Suite struct EdgeCase {}
        }
    }

    // MARK: - Synthetic completion resource

    extension Kernel.Thread.Executor.Completion.Test {
        /// A `Kernel.Completion` with no platform backend: `flush` blocks on
        /// a condvar until `wakeup.wake()` fires (mirroring the eventfd
        /// wakeup semantics of a real proactor), `drain` always delivers
        /// zero events, notification is absent, and `close` is a no-op.
        fileprivate static func syntheticKernel() -> Kernel.Completion {
            let wakes = KernelThreadTest.Harness(0)
            let consumed = LockedBox<Int>(0)
            let driver = Kernel.Completion.Driver(
                submit: { _, _ in },
                flush: {
                    let target = consumed.withLock { $0 } + 1
                    // Timeout is a liveness guard only: on expiry flush just
                    // returns and the run loop re-checks shutdown.
                    try? wakes.wait(until: { $0 >= target }, timeoutSeconds: 1)
                    consumed.withLock { $0 = target }
                    return .zero
                },
                drain: { _ in .zero },
                close: {}
            )
            return Kernel.Completion(
                driver: driver,
                wakeup: Kernel.Wakeup.Channel(signal: { wakes.update { $0 += 1 } }),
                notification: nil,
                capabilities: .init()
            )
        }
    }

    // MARK: - Unit Tests

    extension Kernel.Thread.Executor.Completion.Test.Unit {
        @Test
        func `construct and shutdown with synthetic kernel completes`() {
            let executor = unsafe Kernel.Thread.Executor.Completion(
                kernel: Kernel.Thread.Executor.Completion.Test.syntheticKernel()
            ) { _ in .continue }
            executor.shutdown()
            // No hang = success
        }
    }

    // MARK: - Edge Case: F-001-shape post-shutdown enqueue vs. still-draining run loop

    extension Kernel.Thread.Executor.Completion.Test.EdgeCase {
        /// fable-448 pre-review (Session 2) found the LITERAL F-001
        /// inline-overlap race here, unfixed and undisclosed by the original
        /// F-001 remediation. Pre-fix, `enqueue` ran a post-shutdown job
        /// INLINE on the calling thread the instant `_shutdown.isSet` was
        /// observed true -- even while the executor's own OS thread was
        /// still draining a prior job inside `drainJobs()`. Two jobs of the
        /// same serial executor could then be mid-execution at once (one on
        /// the executor thread, one inline on the caller), violating the
        /// serial-execution guarantee. rev-1 mirrors the
        /// `Kernel.Thread.Executor` fix and its regression test: a
        /// `_loopExited` flag published under `queueLock` only in the
        /// terminal `drainJobs()` call's critical section; `enqueue` queues
        /// normally (letting the drain pick the job up) whenever the loop
        /// has not yet exited, and only runs inline once no executor-thread
        /// concurrency is possible.
        @Test
        func `enqueue during shutdown drain never overlaps a still-draining job`() async throws {
            struct State {
                var job1Running = false
                var releaseJob1 = false
            }
            let harness = KernelThreadTest.Harness(State())
            let executor = unsafe Kernel.Thread.Executor.Completion(
                kernel: Kernel.Thread.Executor.Completion.Test.syntheticKernel()
            ) { _ in .continue }
            let activeJobs = LockedBox<Int>(0)
            let concurrentExecutionDetected = LockedBox<Bool>(false)

            // Job 1 occupies the executor thread until explicitly released
            // below, guaranteeing the run loop has NOT exited (still
            // "draining") for the entire window in which Job 2 races the
            // shutdown call.
            Task(executorPreference: executor) {
                activeJobs.withLock { $0 += 1 }
                harness.update { $0.job1Running = true }
                try? harness.wait(until: { $0.releaseJob1 }, timeoutSeconds: 10)
                activeJobs.withLock { $0 -= 1 }
            }

            try harness.wait(until: { $0.job1Running }, timeoutSeconds: 10)

            // Shut down from a dedicated OS thread: it sets the shutdown
            // flag almost immediately, then blocks in `handle.join()` until
            // Job 1 (still running above) finishes and the run loop
            // actually exits.
            let shutdownThread = try Kernel.Thread.spawn {
                executor.shutdown()
            }

            // Give the shutdown flag time to flip. Job 1 stays blocked for
            // the rest of the test, so this only widens the race window --
            // it cannot close it.
            try await Task.sleep(for: .milliseconds(100))

            // Job 2 races the still-draining executor. Pre-fix: `enqueue`
            // sees `_shutdown.isSet == true` and runs this job INLINE,
            // synchronously, right here on the test's thread -- concurrently
            // with Job 1, which is still occupying the executor thread.
            // Post-fix: `enqueue` sees the run loop has not exited and
            // queues the job normally; it can only run after Job 1
            // finishes, never overlapping it.
            Task(executorPreference: executor) {
                let depth = activeJobs.withLock {
                    $0 += 1
                    return $0
                }
                if depth > 1 {
                    concurrentExecutionDetected.withLock { $0 = true }
                }
                activeJobs.withLock { $0 -= 1 }
            }

            // Snapshot BEFORE releasing Job 1: pre-fix, Job 2 already ran
            // inline (synchronously, as part of the
            // `Task(executorPreference:)` call above) by this point.
            // Post-fix, Job 2 is still queued.
            let overlapObserved = concurrentExecutionDetected.withLock { $0 }

            harness.update { $0.releaseJob1 = true }
            // Best-effort join: a failure here is non-actionable -- the
            // shutdown thread has still run `executor.shutdown()` to
            // completion by the time `pthread_join` returns any error
            // other than success.
            do throws(Kernel.Thread.Error) {
                try shutdownThread.join()
            } catch {
            }

            #expect(overlapObserved == false)
        }
    }

#endif

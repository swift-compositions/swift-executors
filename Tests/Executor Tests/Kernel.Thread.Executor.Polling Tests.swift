//
//  Kernel.Thread.Executor.Polling Tests.swift
//  swift-executors
//

import Kernel_Test_Support
import Testing

@testable import Executors

// Behavioral coverage of a REAL kernel-backed event loop (actual I/O event
// delivery) remains with swift-io (Phase 3). The fable-448 rev-1 tests below
// are written to exercise only the executor's enqueue/shutdown teardown
// contract, using a fully synthetic `Kernel.Event.Driver` (public
// closure-based init) -- no platform kernel resources -- but see the
// investigation note immediately below: on this machine, constructing this
// executor at all reproducibly SIGSEGVs, so the tests that would construct
// it are marked `.disabled` rather than deleted or silently left out.
//
// rev-1 investigation note (pre-existing, out of scope here; NOT introduced
// or fixed by this revision): constructing `Kernel.Thread.Executor.Polling`
// SIGSEGVs the test process on construct+shutdown alone, reproducibly,
// sandboxed and unsandboxed, pre-fix and post-fix -- identically for the
// REAL Darwin backend (`Kernel.Event.Source.kqueue()`, per the Session 2
// predecessor's finding) AND for the fully synthetic closure-based
// `Kernel.Event.Driver` used below (confirmed this session: the minimal
// "construct and shutdown" test crashes with signal 11 on every run, in
// isolation, with zero platform kernel resources involved). Because the
// synthetic driver crashes identically to the real backend, the defect is
// NOT specific to kqueue; it implicates `Kernel.Event.Source`'s own
// construction/close path (swift-kernel) or the `Executor.Wait.Event.Source`
// wrapper around it, not this package's `_loopExited` fix and not the driver
// injection seam itself. For comparison, `Kernel.Thread.Executor.Completion`'s
// equivalent synthetic-`Kernel.Completion` tests (same file shape, same
// harness, same rev-1 change) run and pass cleanly -- which narrows the
// defect specifically to the event-source path, not to executor
// construction/teardown in general. Not chased further here per the
// orchestrator's standing directive (morning-queue material); disclosed in
// O/remediation/swift-executors/REPORT.md for routing. The tests below are
// `.disabled` (not deleted) so their intent and exact assertions stay
// documented and ready to re-enable once the construction defect is fixed
// upstream.

#if !os(Windows)

    extension Kernel.Thread.Executor.Polling {
        enum Test {
            @Suite struct Unit {}
            @Suite struct EdgeCase {}
        }
    }

    // MARK: - Synthetic event source

    extension Kernel.Thread.Executor.Polling.Test {
        /// A `Kernel.Event.Source` with no platform backend: `poll` blocks on
        /// a condvar until `wakeup.wake()` fires (mirroring eventfd/kqueue
        /// wakeup semantics), always delivers zero events, and `close` is a
        /// no-op. Registration paths are never exercised by these tests.
        fileprivate static func syntheticSource() -> Kernel.Event.Source {
            let wakes = KernelThreadTest.Harness(0)
            let consumed = LockedBox<Int>(0)
            let driver = Kernel.Event.Driver(
                add: { _, _, _ in },
                modify: { _, _, _, _ in },
                remove: { _, _, _ in },
                arm: { _, _, _ in },
                poll: { _, _ in
                    let target = consumed.withLock { $0 } + 1
                    // Timeout is a liveness guard only: on expiry poll just
                    // returns 0 and the run loop re-checks shutdown.
                    try? wakes.wait(until: { $0 >= target }, timeoutSeconds: 1)
                    consumed.withLock { $0 = target }
                    return 0
                },
                close: {}
            )
            return Kernel.Event.Source(
                driver: driver,
                wakeup: Kernel.Wakeup.Channel(signal: { wakes.update { $0 += 1 } })
            )
        }
    }

    // MARK: - Unit Tests

    extension Kernel.Thread.Executor.Polling.Test.Unit {
        @Test
        func `Polling.Outcome enum exists`() {
            _ = Kernel.Thread.Executor.Polling.Outcome.continue
            _ = Kernel.Thread.Executor.Polling.Outcome.halt
        }

        @Test(
            .disabled(
                "Pre-existing defect (out of scope for fable-448): constructing Kernel.Thread.Executor.Polling SIGSEGVs the test process on this machine, even with a fully synthetic Kernel.Event.Driver -- see the file-level rev-1 investigation note and O/remediation/swift-executors/REPORT.md."
            )
        )
        func `construct and shutdown with synthetic source completes`() {
            let executor = unsafe Kernel.Thread.Executor.Polling(
                source: Kernel.Thread.Executor.Polling.Test.syntheticSource()
            ) { _ in .continue }
            executor.shutdown()
            // No hang = success
        }
    }

    // MARK: - Edge Case: F-001-shape post-shutdown enqueue vs. still-draining run loop

    extension Kernel.Thread.Executor.Polling.Test.EdgeCase {
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
        ///
        /// Written as a full behavioral regression test (parallel to the
        /// `Kernel.Thread.Executor` and `Completion` EdgeCase tests) but
        /// `.disabled`: it constructs a `Kernel.Thread.Executor.Polling`,
        /// which reproducibly SIGSEGVs on this machine even with the
        /// synthetic driver above -- see the file-level rev-1 investigation
        /// note. Kept (not deleted) so the exact assertion this fix requires
        /// is documented and ready to re-enable once the construction defect
        /// is fixed upstream.
        @Test(
            .disabled(
                "Pre-existing defect (out of scope for fable-448): constructing Kernel.Thread.Executor.Polling SIGSEGVs the test process on this machine, even with a fully synthetic Kernel.Event.Driver -- see the file-level rev-1 investigation note and O/remediation/swift-executors/REPORT.md."
            )
        )
        func `enqueue during shutdown drain never overlaps a still-draining job`() async throws {
            struct State {
                var job1Running = false
                var releaseJob1 = false
            }
            let harness = KernelThreadTest.Harness(State())
            let executor = unsafe Kernel.Thread.Executor.Polling(
                source: Kernel.Thread.Executor.Polling.Test.syntheticSource()
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

//
//  Kernel.Thread.Executor Tests.swift
//  swift-executors
//

import Kernel_Test_Support
import Testing

@testable import Executors

extension Kernel.Thread.Executor {
    enum Test {
        @Suite struct Unit {}
        @Suite struct `Edge Case` {}
        @Suite struct Integration {}
        @Suite(.serialized) struct Performance {}
    }
}

// MARK: - Unit Tests

extension Kernel.Thread.Executor.Test.Unit {
    @Test
    func `executor conforms to SerialExecutor`() {
        let executor = Kernel.Thread.Executor()
        let unowned = executor.asUnownedSerialExecutor()
        // If this compiles and runs, the executor conforms
        _ = unowned
        executor.shutdown()
    }

    @Test
    func `executor conforms to TaskExecutor`() async {
        let executor = Kernel.Thread.Executor()

        // Task(executorPreference:) only works with TaskExecutor
        await Task(executorPreference: executor) {
            // Job executed on executor
        }.value

        executor.shutdown()
    }

    @Test
    func `shutdown completes gracefully`() {
        let executor = Kernel.Thread.Executor()
        executor.shutdown()
        // No hang = success
    }
}

// MARK: - Edge Case: F-001 post-shutdown enqueue vs. still-draining run loop

extension Kernel.Thread.Executor.Test.`Edge Case` {
    /// fable-448 F-001: pre-fix, `enqueue` ran a post-shutdown job INLINE on
    /// the calling thread the instant `_shutdown.isSet` was observed true —
    /// even while the executor's own OS thread was still draining a prior
    /// job. Two jobs of the same serial executor could then be mid-execution
    /// at once (one on the executor thread, one inline on the caller),
    /// violating the serial-execution guarantee. The fix tracks a
    /// `_loopExited` flag set under the same lock only once the run loop has
    /// verifiably returned; `enqueue` now queues normally (letting the drain
    /// pick the job up) whenever the loop has not yet exited, and only runs
    /// inline once no concurrency is possible.
    @Test
    func `enqueue during shutdown drain never overlaps a still-draining job`() async throws {
        struct State {
            var job1Running = false
            var releaseJob1 = false
        }
        let harness = KernelThreadTest.Harness(State())
        let executor = Kernel.Thread.Executor()
        let activeJobs = LockedBox<Int>(0)
        let concurrentExecutionDetected = LockedBox<Bool>(false)

        // Job 1 occupies the executor thread until explicitly released below,
        // guaranteeing the run loop has NOT exited (still "draining") for the
        // entire window in which Job 2 races the shutdown call.
        Task(executorPreference: executor) {
            activeJobs.withLock { $0 += 1 }
            harness.update { $0.job1Running = true }
            try? harness.wait(until: { $0.releaseJob1 }, timeoutSeconds: 10)
            activeJobs.withLock { $0 -= 1 }
        }

        try harness.wait(until: { $0.job1Running }, timeoutSeconds: 10)

        // Shut down from a dedicated OS thread: it sets the shutdown flag
        // almost immediately, then blocks in `handle.join()` until Job 1
        // (still running above) finishes and the run loop actually exits.
        let shutdownThread = try Kernel.Thread.spawn {
            executor.shutdown()
        }

        // Give the shutdown flag time to flip. Job 1 stays blocked for the
        // rest of the test, so this only widens the race window -- it cannot
        // close it.
        try await Task.sleep(for: .milliseconds(100))

        // Job 2 races the still-draining executor. Pre-fix: `enqueue` sees
        // `_shutdown.isSet == true` and runs this job INLINE, synchronously,
        // right here on the test's thread -- concurrently with Job 1, which
        // is still occupying the executor thread. Post-fix: `enqueue` sees
        // the run loop has not exited and queues the job normally; it can
        // only run after Job 1 finishes, never overlapping it.
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

        // Snapshot BEFORE releasing Job 1: pre-fix, Job 2 already ran inline
        // (synchronously, as part of the `Task(executorPreference:)` call
        // above) by this point. Post-fix, Job 2 is still queued.
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

extension Kernel.Thread.Executor.Test.Integration {
    @Test
    func `task executor preference executes on thread`() async {
        let executor = Kernel.Thread.Executor()

        // Use a Sendable result to verify execution
        let result = await Task(executorPreference: executor) {
            return 42
        }.value

        #expect(result == 42)
        executor.shutdown()
    }

    @Test
    func `multiple tasks execute sequentially on same executor`() async {
        let executor = Kernel.Thread.Executor()

        let r1 = await Task(executorPreference: executor) { 1 }.value
        let r2 = await Task(executorPreference: executor) { 2 }.value
        let r3 = await Task(executorPreference: executor) { 3 }.value

        #expect(r1 == 1)
        #expect(r2 == 2)
        #expect(r3 == 3)
        executor.shutdown()
    }
}

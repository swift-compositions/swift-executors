import Kernel_Test_Support
import Testing

@testable import Executors

#if !os(Windows)

    extension Kernel.Thread.Executor.Polling {
        enum Test {
            @Suite struct Unit {}
            @Suite struct EdgeCase {}
        }
    }

    extension Kernel.Thread.Executor.Polling.Test {

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

        }
    }

    extension Kernel.Thread.Executor.Polling.Test.EdgeCase {

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

            Task(executorPreference: executor) {
                activeJobs.withLock { $0 += 1 }
                harness.update { $0.job1Running = true }
                try? harness.wait(until: { $0.releaseJob1 }, timeoutSeconds: 10)
                activeJobs.withLock { $0 -= 1 }
            }

            try harness.wait(until: { $0.job1Running }, timeoutSeconds: 10)

            let shutdownThread = try Kernel.Thread.spawn {
                executor.shutdown()
            }

            try await Task.sleep(for: .milliseconds(100))

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

            let overlapObserved = concurrentExecutionDetected.withLock { $0 }

            harness.update { $0.releaseJob1 = true }

            do throws(Kernel.Thread.Error) {
                try shutdownThread.join()
            } catch {
            }

            #expect(overlapObserved == false)
        }
    }

#endif

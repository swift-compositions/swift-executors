import Kernel_Test_Support
import Testing

@testable import Executors

#if !os(Windows)

    extension Kernel.Thread.Executor.Completion {
        enum Test {
            @Suite struct Unit {}
            @Suite struct EdgeCase {}
        }
    }

    extension Kernel.Thread.Executor.Completion.Test {

        fileprivate static func syntheticKernel() -> Kernel.Completion {
            let wakes = KernelThreadTest.Harness(0)
            let consumed = LockedBox<Int>(0)
            let driver = Kernel.Completion.Driver(
                submit: { _, _ in },
                flush: {
                    let target = consumed.withLock { $0 } + 1

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

    extension Kernel.Thread.Executor.Completion.Test.Unit {
        @Test
        func `construct and shutdown with synthetic kernel completes`() {
            let executor = unsafe Kernel.Thread.Executor.Completion(
                kernel: Kernel.Thread.Executor.Completion.Test.syntheticKernel()
            ) { _ in .continue }
            executor.shutdown()

        }
    }

    extension Kernel.Thread.Executor.Completion.Test.EdgeCase {

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

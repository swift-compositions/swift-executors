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

extension Kernel.Thread.Executor.Test.Unit {
    @Test
    func `executor conforms to SerialExecutor`() {
        let executor = Kernel.Thread.Executor()
        let unowned = unsafe executor.asUnownedSerialExecutor()

        _ = unsafe unowned
        executor.shutdown()
    }

    @Test
    func `executor conforms to TaskExecutor`() async {
        let executor = Kernel.Thread.Executor()

        await Task(executorPreference: executor) {

        }.value

        executor.shutdown()
    }

    @Test
    func `shutdown completes gracefully`() {
        let executor = Kernel.Thread.Executor()
        executor.shutdown()

    }
}

extension Kernel.Thread.Executor.Test.`Edge Case` {

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

extension Kernel.Thread.Executor.Test.Integration {
    @Test
    func `task executor preference executes on thread`() async {
        let executor = Kernel.Thread.Executor()

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

#if canImport(Darwin)
    import Darwin

    extension Kernel.Thread.Executor.Test.Integration {
        /// The threads of this process, as the kernel counts them.
        static func threads() -> Int {
            var list: thread_act_array_t?
            var count: mach_msg_type_number_t = 0
            guard unsafe task_threads(mach_task_self_, &list, &count) == KERN_SUCCESS, let list = unsafe list else { return 0 }
            unsafe UnsafeBufferPointer(start: list, count: Int(count)).forEach { _ = mach_port_deallocate(mach_task_self_, $0) }
            let size = vm_size_t(MemoryLayout<thread_t>.stride * Int(count))
            _ = unsafe vm_deallocate(mach_task_self_, vm_address_t(bitPattern: list), size)
            return Int(count)
        }

        /// The threads before, while `count` values `make` gives are held, and after they are let go and
        /// their threads have had up to five seconds to end.
        static func threads(holding count: Int, _ make: () -> AnyObject) async throws -> (before: Int, held: Int, after: Int) {
            let before = threads()
            var made = (0..<count).map { _ in make() }
            let held = threads()
            made.removeAll()
            var after = threads()
            for _ in 0..<100 where after > before + 32 {
                try await Task.sleep(for: .milliseconds(50))
                after = threads()
            }
            return (before, held, after)
        }

        @Test
        func `an executor's thread ends when its last reference goes, without shutdown`() async throws {
            let before = Self.threads()
            for _ in 0..<200 {
                let executor = Kernel.Thread.Executor()
                await Task(executorPreference: executor) {}.value
            }
            let made = (0..<200).map { _ in Kernel.Thread.Executor() }
            #expect(Self.threads() >= before + 150)
            _ = consume made
            var after = Self.threads()
            for _ in 0..<100 where after > before + 64 {
                try await Task.sleep(for: .milliseconds(50))
                after = Self.threads()
            }
            #expect(before > 0)
            #expect(after <= before + 64, "\(before) threads before, \(after) after 200 executors were made and let go")
        }
    }
#endif

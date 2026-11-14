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

#if !os(Windows)
    #if canImport(Darwin)
        import Darwin
    #elseif canImport(Glibc)
        import Glibc
    #elseif canImport(Musl)
        import Musl
    #endif
    import Dispatch

    extension Kernel.Thread.Executor.Test {
        /// A token a thread holds from when it is planted until the thread ends: the thread's own exit releases
        /// it, through a thread-specific value's destructor, and its release signals `ended`.
        final class Ending: Sendable {
            let ended: DispatchSemaphore

            init(_ ended: DispatchSemaphore) {
                self.ended = ended
            }

            deinit {
                ended.signal()
            }
        }
    }

    extension Kernel.Thread.Executor.Test.Ending {
        /// The key whose destructor releases a thread's token as the thread ends.
        static let key: pthread_key_t = {
            var key = pthread_key_t()
            #if canImport(Darwin)
                _ = unsafe pthread_key_create(&key) { token in
                    unsafe Unmanaged<Kernel.Thread.Executor.Test.Ending>.fromOpaque(token).release()
                }
            #else
                _ = unsafe pthread_key_create(&key) { token in
                    guard let token = unsafe token else { return }
                    unsafe Unmanaged<Kernel.Thread.Executor.Test.Ending>.fromOpaque(token).release()
                }
            #endif
            return key
        }()

        /// Leaves a token on the calling thread, signalling `ended` when the thread ends; false when the thread
        /// holds one already.
        static func plant(_ ended: DispatchSemaphore) -> Bool {
            guard unsafe pthread_getspecific(key) == nil else { return false }
            _ = unsafe pthread_setspecific(key, Unmanaged.passRetained(Self(ended)).toOpaque())
            return true
        }

        /// A token left on the thread a task on `executor` runs on.
        static func plant(on executor: any TaskExecutor, _ ended: DispatchSemaphore) async -> Bool {
            await Task(executorPreference: executor) { plant(ended) }.value
        }

        /// How many of `count` threads ended within `seconds`, all told.
        static func count(_ ended: DispatchSemaphore, _ count: Int, within seconds: Double) -> Int {
            let deadline = DispatchTime.now() + seconds
            var seen = 0
            while seen < count, ended.wait(timeout: deadline) == .success { seen += 1 }
            return seen
        }
    }

    extension Kernel.Thread.Executor.Test.Integration {
        @Test
        func `each executor's thread lives while it is held and ends when its last reference goes, without shutdown`() async {
            let ended = DispatchSemaphore(value: 0)
            var held = (0..<50).map { _ in Kernel.Thread.Executor() }
            for executor in held {
                #expect(await Kernel.Thread.Executor.Test.Ending.plant(on: executor, ended))
            }
            #expect(Kernel.Thread.Executor.Test.Ending.count(ended, 1, within: 0) == 0)
            held.removeAll()
            #expect(Kernel.Thread.Executor.Test.Ending.count(ended, 50, within: 10) == 50)
        }
    }
#endif

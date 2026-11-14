import Executors
import Kernel_Test_Support
import Synchronization
import Testing

extension Kernel.Thread.Executor.Stealing {
    enum Test {
        @Suite struct Unit {}
    }
}

private final class OneShot: Sendable {
    private let resumed = Atomic<Bool>(false)

    func claim() -> Bool {
        !resumed.exchange(true, ordering: .sequentiallyConsistent)
    }
}

private func withDeadline<T: Sendable>(
    _ deadline: Duration = .seconds(15),
    _ operation: @escaping @Sendable () async -> T
) async -> T? {
    let gate = OneShot()
    return await withCheckedContinuation { (continuation: CheckedContinuation<T?, Never>) in
        Task {
            let value = await operation()
            if gate.claim() { continuation.resume(returning: value) }
        }
        Task {
            try? await Task.sleep(for: deadline)
            if gate.claim() { continuation.resume(returning: nil) }
        }
    }
}

extension Kernel.Thread.Executor.Stealing.Test.Unit {
    @Test
    func `stealing pool creates and shuts down`() {
        let pool = Kernel.Thread.Executor.Stealing(.init(count: 2))
        pool.shutdown()
    }

    @Test(.timeLimit(.minutes(2)))
    func `enqueue after every worker has exited runs the job instead of hanging`() async throws {
        let pool = Kernel.Thread.Executor.Stealing(.init(count: 2))
        pool.shutdown()

        let result = await withDeadline {
            await Task(executorPreference: pool) { 42 }.value
        }
        #expect(result == 42)
    }

    @Test
    func `priorityTracking defaults to false`() {
        let options = Kernel.Thread.Executor.Stealing.Options(count: 2)
        #expect(options.priorityTracking == false)
    }

    @Test
    func `priorityTracking true runs jobs correctly`() async {
        let pool = Kernel.Thread.Executor.Stealing(
            .init(count: 2, priorityTracking: true)
        )
        let result = await Task(executorPreference: pool) { 42 }.value
        #expect(result == 42)
        pool.shutdown()
    }
}

#if !os(Windows)
    import Dispatch

    extension Kernel.Thread.Executor.Stealing.Test.Unit {
        /// Leaves a token on each of a pool's `count` threads: `count` tasks that each wait until all have
        /// arrived, so each holds a thread of its own while it plants.
        static func plant(on pool: Kernel.Thread.Executor.Stealing, count: Int, _ ended: DispatchSemaphore) async -> Int {
            let arrived = DispatchSemaphore(value: 0)
            let gate = DispatchSemaphore(value: 0)
            return await withTaskGroup(of: Bool.self) { group in
                for _ in 0..<count {
                    group.addTask(executorPreference: pool) {
                        let planted = Kernel.Thread.Executor.Test.Ending.plant(ended)
                        arrived.signal()
                        return planted && gate.wait(timeout: .now() + 10) == .success
                    }
                }
                if Kernel.Thread.Executor.Test.Ending.count(arrived, count, within: 10) == count {
                    for _ in 0..<count { gate.signal() }
                }
                var planted = 0
                for await done in group where done { planted += 1 }
                return planted
            }
        }

        @Test
        func `each stealing pool's threads live while it is held and end when its last reference goes, without shutdown`() async {
            let ended = DispatchSemaphore(value: 0)
            var held = (0..<10).map { _ in Kernel.Thread.Executor.Stealing(.init(count: 4)) }
            for pool in held {
                #expect(await Self.plant(on: pool, count: 4, ended) == 4)
            }
            #expect(Kernel.Thread.Executor.Test.Ending.count(ended, 1, within: 0) == 0)
            held.removeAll()
            #expect(Kernel.Thread.Executor.Test.Ending.count(ended, 40, within: 10) == 40)
        }
    }
#endif

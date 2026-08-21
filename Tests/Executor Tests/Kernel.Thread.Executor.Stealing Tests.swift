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

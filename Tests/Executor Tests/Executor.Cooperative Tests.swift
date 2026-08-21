import Synchronization
import Testing

@testable import Executors

extension Executor.Cooperative {
    enum Test {
        @Suite struct Unit {}
        @Suite struct Integration {}
    }
}

private actor Cooperator {
    nonisolated let cooperative: Executor.Cooperative
    var value: Int = 0

    init(_ cooperative: Executor.Cooperative) {
        self.cooperative = cooperative
    }
}

extension Cooperator {
    nonisolated var unownedExecutor: UnownedSerialExecutor {
        cooperative.asUnownedSerialExecutor()
    }

    func increment() { value += 1 }
}

private final class OneShot: Sendable {
    private let resumed = Atomic<Bool>(false)

    func claim() -> Bool {
        !resumed.exchange(true, ordering: .sequentiallyConsistent)
    }
}

private func withDeadline<T: Sendable>(
    _ deadline: Duration = .seconds(5),
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

extension Executor.Cooperative.Test.Unit {
    @Test
    func `create and shutdown`() {
        let executor = Executor.Cooperative()
        executor.shutdown()
    }

    @Test
    func `stop without prior run is a no-op`() {
        let executor = Executor.Cooperative()
        executor.stop()
        executor.shutdown()
    }

    @Test
    func `runUntil returns immediately when condition is already true`() {
        let executor = Executor.Cooperative()
        executor.runUntil { true }
        executor.shutdown()
    }

    @Test
    func `enqueue after shutdown asserts in debug builds only`() async {
        if _isDebugAssertConfiguration() {
            await #expect(processExitsWith: .failure) {
                let executor = Executor.Cooperative()
                executor.shutdown()
                let helper = Cooperator(executor)

                _ = await withDeadline(.seconds(5)) { await helper.increment() }
            }
        } else {
            await #expect(processExitsWith: .success) {
                let executor = Executor.Cooperative()
                executor.shutdown()
                let helper = Cooperator(executor)

                _ = await withDeadline(.seconds(2)) { await helper.increment() }
            }
        }
    }
}

extension Executor.Cooperative.Test.Integration {
    @Test
    func `run returns on shutdown from another thread`() async {
        let executor = Executor.Cooperative()

        let thread = Kernel.Thread.Handle.Reference(
            Kernel.Thread.trap { executor.run() }
        )

        try? await Task.sleep(for: .milliseconds(50))
        executor.shutdown()

        do throws(Kernel.Thread.Error) {
            try thread.join()
        } catch {
        }
    }

    @Test
    func `stop causes run to return`() async {
        let executor = Executor.Cooperative()

        let thread = Kernel.Thread.Handle.Reference(
            Kernel.Thread.trap { executor.run() }
        )

        try? await Task.sleep(for: .milliseconds(50))
        executor.stop()

        do throws(Kernel.Thread.Error) {
            try thread.join()
        } catch {
        }

        executor.shutdown()
    }

    @Test
    func `stop from another thread causes runUntil to return`() async {
        let executor = Executor.Cooperative()

        let thread = Kernel.Thread.Handle.Reference(
            Kernel.Thread.trap { executor.runUntil { false } }
        )

        try? await Task.sleep(for: .milliseconds(50))
        executor.stop()

        do throws(Kernel.Thread.Error) {
            try thread.join()
        } catch {
        }
        executor.shutdown()
    }

    @Test
    func `actor method runs on donated thread`() async {
        let executor = Executor.Cooperative()
        let helper = Cooperator(executor)

        let thread = Kernel.Thread.Handle.Reference(
            Kernel.Thread.trap { executor.run() }
        )

        try? await Task.sleep(for: .milliseconds(50))

        await helper.increment()
        let result = await helper.value
        #expect(result == 1)

        executor.shutdown()

        do throws(Kernel.Thread.Error) {
            try thread.join()
        } catch {
        }
    }

    @Test
    func `shutdown dominates stop`() async {
        let executor = Executor.Cooperative()

        let thread = Kernel.Thread.Handle.Reference(
            Kernel.Thread.trap { executor.run() }
        )

        try? await Task.sleep(for: .milliseconds(50))

        executor.stop()
        executor.shutdown()

        do throws(Kernel.Thread.Error) {
            try thread.join()
        } catch {
        }
    }
}

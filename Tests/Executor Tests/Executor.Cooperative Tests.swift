//
//  Executor.Cooperative Tests.swift
//  swift-executors
//

import Synchronization
import Testing

@testable import Executors

extension Executor.Cooperative {
    enum Test {
        @Suite struct Unit {}
        @Suite struct Integration {}
    }
}

/// Actor pinned to a cooperative executor for enqueue-via-actor tests.
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

/// Awaits an operation with a hard deadline, returning `nil` on timeout.
/// A continuation race that abandons (leaks) the losing task rather than
/// awaiting it -- an abandoned post-shutdown job can never resume, so any
/// structured await on it (task group, async let) would hang instead of
/// timing out. The leak is confined to the (exit-test child) process and
/// does not block its exit. Same rationale as the helper in
/// `Kernel.Thread.Executor.Stealing Tests.swift`.
private final class OneShot: Sendable {
    private let resumed = Atomic<Bool>(false)

    /// `true` for exactly one caller, ever.
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

// MARK: - Unit Tests

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

    /// fable-448 F-002: `Cooperative.enqueue` never checked `_shutdown` at
    /// all -- a post-shutdown job silently joined the `jobs` queue, was
    /// never drained (the run loop had already exited and does not
    /// restart), and its continuation never resumed, hanging any awaiter
    /// indefinitely. Rather than trap unconditionally (a release-mode
    /// behavior change that could break existing best-effort callers), the
    /// fix documents the abandonment consequence and adds a debug-only
    /// `assertionFailure` so the mistake is loud during development while
    /// release builds keep the documented silent-abandon contract. Debug
    /// builds must therefore crash (`.failure`); release builds must not
    /// (`.success`) -- this is a genuine behavior fork by build
    /// configuration, not a test artifact, so both arms are asserted here.
    @Test
    func `enqueue after shutdown asserts in debug builds only`() async {
        if _isDebugAssertConfiguration() {
            await #expect(processExitsWith: .failure) {
                let executor = Executor.Cooperative()
                executor.shutdown()
                let helper = Cooperator(executor)
                // The actor hop enqueues onto the shut-down executor. The
                // debug assert must abort the child well before the
                // deadline; if it does not (pre-fix), the abandoned job's
                // await times out and the child exits 0 -- failing the
                // `.failure` expectation.
                _ = await withDeadline(.seconds(5)) { await helper.increment() }
            }
        } else {
            await #expect(processExitsWith: .success) {
                let executor = Executor.Cooperative()
                executor.shutdown()
                let helper = Cooperator(executor)
                // Release: the job is silently abandoned (the documented
                // contract); the awaiting task never resumes and the
                // deadline path exits 0.
                _ = await withDeadline(.seconds(2)) { await helper.increment() }
            }
        }
    }
}

// MARK: - Donation Contract

extension Executor.Cooperative.Test.Integration {
    @Test
    func `run returns on shutdown from another thread`() async {
        let executor = Executor.Cooperative()

        let thread = Kernel.Thread.Handle.Reference(
            Kernel.Thread.trap { executor.run() }
        )

        try? await Task.sleep(for: .milliseconds(50))
        executor.shutdown()
        thread.join()
    }

    @Test
    func `stop causes run to return`() async {
        let executor = Executor.Cooperative()

        let thread = Kernel.Thread.Handle.Reference(
            Kernel.Thread.trap { executor.run() }
        )

        try? await Task.sleep(for: .milliseconds(50))
        executor.stop()
        thread.join()

        // Executor is still usable after stop (non-destructive)
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
        thread.join()
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
        thread.join()
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
        thread.join()
    }
}

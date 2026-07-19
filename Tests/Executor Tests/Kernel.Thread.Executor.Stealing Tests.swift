//
//  Kernel.Thread.Executor.Stealing Tests.swift
//  swift-executors
//

import Executors
import Kernel_Test_Support
import Testing

extension Kernel.Thread.Executor.Stealing {
    enum Test {
        @Suite struct Unit {}
    }
}

/// Awaits an operation with a hard deadline, returning `nil` on timeout
/// instead of hanging the whole test binary. `.timeLimit` alone does not
/// bound this: it cancels the *test function's own* task, not a separately
/// spawned unstructured `Task`, and `Task<T, Never>.value` does not observe
/// ambient cancellation. See swift-async's
/// `Async.Stream.Lifecycle Tests.swift` (fable-448 F-001/F-002) for the same
/// pattern applied to a different hang.
private func withDeadline<T: Sendable>(
    _ deadline: Duration = .seconds(15),
    _ operation: @escaping @Sendable () async -> T
) async -> T? {
    await withTaskGroup(of: T?.self) { group in
        group.addTask { await operation() }
        group.addTask {
            try? await Task.sleep(for: deadline)
            return nil
        }
        let first = await group.next()!
        group.cancelAll()
        return first
    }
}

extension Kernel.Thread.Executor.Stealing.Test.Unit {
    @Test
    func `stealing pool creates and shuts down`() {
        let pool = Kernel.Thread.Executor.Stealing(.init(count: 2))
        pool.shutdown()
    }

    /// fable-448 F-002: pre-fix, `Stealing.enqueue` pushed onto a worker's
    /// deque unconditionally, with no shutdown check at all. `pool.shutdown()`
    /// joins every worker thread before returning, so by the time it returns
    /// every worker's run loop has verifiably exited -- a job enqueued after
    /// that point was pushed onto a dead worker's deque and NEVER dequeued
    /// (no live worker steals from a peer once it has itself noticed
    /// shutdown), hanging the awaiting task forever. The fix gives each
    /// `Worker` its own `_loopExited` flag, set under its own lock at the end
    /// of its run loop; `enqueue` now runs the job inline once that flag is
    /// set, instead of orphaning it in an abandoned deque.
    @Test(.timeLimit(.minutes(2)))
    func `enqueue after every worker has exited runs the job instead of hanging`() async throws {
        let pool = Kernel.Thread.Executor.Stealing(.init(count: 2))
        pool.shutdown()  // blocks until every worker thread has fully joined

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

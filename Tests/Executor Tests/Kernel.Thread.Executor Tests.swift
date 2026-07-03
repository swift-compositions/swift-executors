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
        @Suite struct EdgeCase {}
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

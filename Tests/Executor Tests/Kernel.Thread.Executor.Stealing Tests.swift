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

extension Kernel.Thread.Executor.Stealing.Test.Unit {
    @Test
    func `stealing pool creates and shuts down`() {
        let pool = Kernel.Thread.Executor.Stealing(.init(count: 2))
        pool.shutdown()
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

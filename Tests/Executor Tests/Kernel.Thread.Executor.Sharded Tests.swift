import Kernel_Test_Support
import Testing

@testable import Executors

extension Kernel.Thread.Executor.Sharded {
    enum Test {
        @Suite struct Unit {}
        @Suite struct `Edge Case` {}
        @Suite struct Integration {}
        @Suite(.serialized) struct Performance {}
    }
}

extension Kernel.Thread.Executor.Sharded.Test.Unit {
    @Test
    func `init with default options creates threads`() {
        let pool = Kernel.Thread.Executor.Sharded()
        #expect(pool.count > 0)
        #expect(pool.count <= 4)
        pool.shutdown()
    }

    @Test
    func `init with custom count`() {
        let pool = Kernel.Thread.Executor.Sharded(.init(count: 2))
        #expect(pool.count == 2)
        pool.shutdown()
    }

    @Test
    func `next returns an executor`() {
        let pool = Kernel.Thread.Executor.Sharded(.init(count: 2))
        let executor = pool.next()
        _ = executor.asUnownedSerialExecutor()
        pool.shutdown()
    }

    @Test
    func `executor(at:) returns executor at index`() {
        let pool = Kernel.Thread.Executor.Sharded(.init(count: 2))
        let e0 = pool.executor(at: 0)
        let e1 = pool.executor(at: 1)
        _ = e0.asUnownedSerialExecutor()
        _ = e1.asUnownedSerialExecutor()
        pool.shutdown()
    }

    @Test
    func `executor(at:) wraps around`() {
        let pool = Kernel.Thread.Executor.Sharded(.init(count: 2))
        let e0 = pool.executor(at: 0)
        let e2 = pool.executor(at: 2)

        _ = e0.asUnownedSerialExecutor()
        _ = e2.asUnownedSerialExecutor()
        pool.shutdown()
    }

    @Test
    func `shutdown completes gracefully`() {
        let pool = Kernel.Thread.Executor.Sharded(.init(count: 2))
        pool.shutdown()

    }

    @Test
    func `priorityTracking defaults to false`() {
        let options = Kernel.Thread.Executor.Sharded.Options(count: 2)
        #expect(options.priorityTracking == false)
    }

    @Test
    func `priorityTracking true runs jobs correctly`() async {
        let pool = Kernel.Thread.Executor.Sharded(
            .init(count: 2, priorityTracking: true)
        )
        let result = await Task(executorPreference: pool.next()) { 42 }.value
        #expect(result == 42)
        pool.shutdown()
    }
}

extension Kernel.Thread.Executor.Sharded.Test.Unit {
    @Test
    func `isIsolatingCurrentContext returns false from non-shard thread`() {
        let pool = Kernel.Thread.Executor.Sharded(.init(count: 2))
        #expect(pool.isIsolatingCurrentContext() == false)
        pool.shutdown()
    }

    @Test
    func `isIsolatingCurrentContext returns true from shard thread`() async {
        let pool = Kernel.Thread.Executor.Sharded(.init(count: 2))
        let shard = pool.next()

        let result = await Task(executorPreference: shard) {
            pool.isIsolatingCurrentContext()
        }.value

        #expect(result == true)
        pool.shutdown()
    }

    @Test
    func `isIsolatingCurrentContext returns false after shutdown`() {
        let pool = Kernel.Thread.Executor.Sharded(.init(count: 2))
        pool.shutdown()
        #expect(pool.isIsolatingCurrentContext() == false)
    }
}

extension Kernel.Thread.Executor.Sharded.Test.Integration {
    @Test
    func `round-robin distributes across executors`() {
        let pool = Kernel.Thread.Executor.Sharded(.init(count: 2))

        let e1 = pool.next()
        let e2 = pool.next()
        let e3 = pool.next()
        let e4 = pool.next()

        let ref1 = ObjectIdentifier(e1)
        let ref2 = ObjectIdentifier(e2)
        let ref3 = ObjectIdentifier(e3)
        let ref4 = ObjectIdentifier(e4)

        #expect(ref1 != ref2)
        #expect(ref1 == ref3)
        #expect(ref2 == ref4)

        pool.shutdown()
    }

    @Test
    func `tasks on different executors run independently`() async {
        let pool = Kernel.Thread.Executor.Sharded(.init(count: 2))

        let e1 = pool.executor(at: 0)
        let e2 = pool.executor(at: 1)

        let result1 = await Task(executorPreference: e1) { 1 }.value
        let result2 = await Task(executorPreference: e2) { 2 }.value

        #expect(result1 == 1)
        #expect(result2 == 2)

        pool.shutdown()
    }
}

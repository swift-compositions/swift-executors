//
//  Executor.Runtime Tests.swift
//  swift-executors
//
//  TX-N1D — law/edge/near-miss fixtures for the additive
//  `Executor.Runtime` interpretation.
//

import Kernel_Test_Support
import Testing

@testable import Executors

extension Executor {
    enum `Runtime Test` {
        @Suite struct Unit {}
        @Suite struct `Edge Case` {}
    }
}

// MARK: - Unit Tests (positive)

extension Executor.`Runtime Test`.Unit {
    @Test
    func `single-thread executor reports running before shutdown`() {
        let executor = Kernel.Thread.Executor()
        #expect(executor.runtime == .running)
        executor.shutdown()
    }

    @Test
    func `single-thread executor reports terminated after shutdown returns`() {
        let executor = Kernel.Thread.Executor()
        executor.shutdown()
        #expect(executor.runtime == .terminated)
    }

    @Test
    func `sharded pool reports running before shutdown`() {
        let pool = Kernel.Thread.Executor.Sharded(.init(count: 2))
        #expect(pool.runtime == .running)
        pool.shutdown()
    }

    @Test
    func `sharded pool reports terminated after shutdown returns`() {
        let pool = Kernel.Thread.Executor.Sharded(.init(count: 2))
        pool.shutdown()
        #expect(pool.runtime == .terminated)
    }

    @Test
    func `stealing pool reports running before shutdown`() {
        let pool = Kernel.Thread.Executor.Stealing(.init(count: 2))
        #expect(pool.runtime == .running)
        pool.shutdown()
    }

    @Test
    func `stealing pool reports terminated after shutdown returns`() {
        let pool = Kernel.Thread.Executor.Stealing(.init(count: 2))
        pool.shutdown()
        #expect(pool.runtime == .terminated)
    }
}

// MARK: - Edge Cases (boundary / terminal-state)

extension Executor.`Runtime Test`.`Edge Case` {
    @Test
    func `sharded pool of one shard aggregates the single shard's state`() {
        let pool = Kernel.Thread.Executor.Sharded(.init(count: 1))
        #expect(pool.runtime == .running)
        pool.shutdown()
        #expect(pool.runtime == .terminated)
    }

    @Test
    func `stealing pool of one worker aggregates the single worker's state`() {
        let pool = Kernel.Thread.Executor.Stealing(.init(count: 1))
        #expect(pool.runtime == .running)
        pool.shutdown()
        #expect(pool.runtime == .terminated)
    }

    @Test
    func `runtime is a pure read -- repeated reads do not change observed state`() {
        let executor = Kernel.Thread.Executor()
        #expect(executor.runtime == .running)
        #expect(executor.runtime == .running)
        executor.shutdown()
        #expect(executor.runtime == .terminated)
        #expect(executor.runtime == .terminated)
    }
}

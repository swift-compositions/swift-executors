//
//  Executor.Affinity Tests.swift
//  swift-executors
//
//  TX-N1D — law/edge/near-miss fixtures for the additive
//  `Executor.Affinity` interpretation.
//

import Kernel_Test_Support
import Testing

@testable import Executors

extension Executor {
    enum `Affinity Test` {
        @Suite struct Unit {}
        @Suite struct `Edge Case` {}
    }
}

// MARK: - Unit Tests (positive)

extension Executor.`Affinity Test`.Unit {
    @Test
    func `sharded pool reports one affinity per shard, in order`() {
        let pool = Kernel.Thread.Executor.Sharded(.init(count: 3))
        let affinities = pool.affinities
        #expect(affinities.count == 3)
        #expect(affinities.map(\.index) == [0, 1, 2])
        #expect(affinities.allSatisfy { $0.count == 3 })
        pool.shutdown()
    }

    @Test
    func `stealing pool reports one affinity per worker, in order`() {
        let pool = Kernel.Thread.Executor.Stealing(.init(count: 3))
        let affinities = pool.affinities
        #expect(affinities.count == 3)
        #expect(affinities.map(\.index) == [0, 1, 2])
        #expect(affinities.allSatisfy { $0.count == 3 })
        pool.shutdown()
    }

    @Test
    func `equal index and count compare equal`() {
        #expect(Executor.Affinity(index: 1, count: 4) == Executor.Affinity(index: 1, count: 4))
    }

    @Test
    func `differing index does not compare equal`() {
        #expect(Executor.Affinity(index: 0, count: 4) != Executor.Affinity(index: 1, count: 4))
    }
}

// MARK: - Edge Cases (zero-length / boundary-capacity)

extension Executor.`Affinity Test`.`Edge Case` {
    @Test
    func `pool of one shard reports a single affinity of count one`() {
        let pool = Kernel.Thread.Executor.Sharded(.init(count: 1))
        #expect(pool.affinities == [Executor.Affinity(index: 0, count: 1)])
        pool.shutdown()
    }

    @Test
    func `pool of one worker reports a single affinity of count one`() {
        let pool = Kernel.Thread.Executor.Stealing(.init(count: 1))
        #expect(pool.affinities == [Executor.Affinity(index: 0, count: 1)])
        pool.shutdown()
    }

    @Test
    func `boundary index equal to count minus one is the last valid position`() {
        let affinity = Executor.Affinity(index: 3, count: 4)
        #expect(affinity.index == 3)
        #expect(affinity.count == 4)
    }
}

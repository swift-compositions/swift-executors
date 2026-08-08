//
//  Executor.Affinity.swift
//  swift-executors
//
//  TX-N1D — portable affinity interpretation.
//
//  `Executor.Affinity` is additive vocabulary the platform leaves in this
//  package already embody informally: `Sharded` pins jobs to one of
//  `count` round-robin threads, and `Stealing` gives every worker a
//  stable identity among `count` peers even though jobs themselves are
//  not pinned. This type gives that existing position/cardinality pair a
//  single, portable name that a leaf can report itself in terms of via
//  an `affinity`/`affinities` accessor -- it does not change how any leaf
//  selects, pins, or steals work, and it holds no platform code of its
//  own (CPU-level affinity, where it exists, stays in the leaf that owns
//  it, e.g. `CPU.Cache.Padded` in `Sharded`).
//

extension Executor {
    /// A portable description of an executor's position among its peers.
    ///
    /// `index` and `count` name the same round-robin/worker-identity
    /// vocabulary already present, informally, in `Sharded` and
    /// `Stealing`. `Affinity` does not confer or change pinning behavior;
    /// it only lets a leaf report the position it already has.
    public struct Affinity: Sendable, Equatable {
        /// This member's position among its peers, in `[0, count)`.
        public var index: Int

        /// The total number of peers this member belongs to.
        public var count: Int

        /// Creates an affinity value.
        ///
        /// - Precondition: `0 <= index < count`.
        public init(index: Int, count: Int) {
            precondition(count > 0, "Executor.Affinity: count must be positive")
            precondition(
                (0..<count).contains(index),
                "Executor.Affinity: index out of range"
            )
            self.index = index
            self.count = count
        }
    }
}

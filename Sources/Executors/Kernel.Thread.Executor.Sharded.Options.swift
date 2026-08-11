//
//  Kernel.Thread.Executor.Sharded.Options.swift
//  swift-executors
//
//  Created by Coen ten Thije Boonkkamp on 28/12/2025.
//

extension Kernel.Thread.Executor.Sharded {
    /// Configuration options for the sharded executor pool.
    public struct Options: Sendable {
        /// Number of executor threads in the pool.
        public var count: Kernel.Thread.Count

        /// Enables per-job thread-QoS tracking on Darwin.
        ///
        /// When `true`, each shard thread's QoS class is bumped to
        /// match the current job's priority for the duration of job
        /// execution via `pthread_override_qos_class_start_np`, then
        /// reverted at job-end. This is the M3 mechanism of the
        /// priority-inversion policy (https://github.com/swift-institute/Research/blob/main/Packages/swift-executors/priority-escalation-policy.md).
        ///
        /// **No-op on Linux, Windows, and Embedded.** These platforms
        /// lack an unprivileged QoS primitive equivalent to Darwin's
        /// pthread override API. The flag is accepted for source
        /// compatibility but produces no runtime effect.
        ///
        /// Default: `false` for v1. Will default to `true` on Darwin
        /// in v2 once the override lifecycle is validated in
        /// production.
        public var priorityTracking: Bool

        /// Creates options with the specified thread count.
        ///
        /// - Parameters:
        ///   - count: Number of threads. If nil, defaults to
        ///     min(4, processorCount).
        ///   - priorityTracking: See the property documentation.
        ///     Default `false`.
        public init(
            count: Kernel.Thread.Count? = nil,
            priorityTracking: Bool = false
        ) {
            self.count =
                count
                ?? Kernel.Thread.Count.min(
                    Self.defaultCount,
                    System.Processor.count.retag(Kernel.Thread.self)
                )
            self.priorityTracking = priorityTracking
        }
    }
}

extension Kernel.Thread.Executor.Sharded.Options {
    private static let defaultCount: Kernel.Thread.Count = 4
}

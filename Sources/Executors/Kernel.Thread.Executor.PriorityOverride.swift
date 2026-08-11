//
//  Kernel.Thread.Executor.PriorityOverride.swift
//  swift-executors
//
//  Drain-path helper for the M3 (Darwin thread-QoS bump) mechanism
//  per https://github.com/swift-institute/Research/blob/main/Packages/swift-executors/priority-escalation-policy.md. Brackets job execution
//  with the Darwin QoS override — `Darwin.Kernel.Thread.QoS.withOverride`,
//  which wraps `pthread_override_qos_class_start_np` / `_end_np` at L2
//  (swift-darwin-standard), reached here transitively via `import Kernel`.
//  This bumps the running job's OS-level thread QoS to match its
//  `ExecutorJob.priority`, implementing the PIP bound of
//  Sha-Rajkumar-Lehoczky 1990 at the executor layer.
//
//  On non-Darwin platforms this reduces to a direct `runSynchronously`
//  call; the flag is accepted for source compatibility but produces
//  no runtime effect (see priority-escalation-policy.md §Rationale).
//

extension Kernel.Thread.Executor {

    /// Run a job on the current thread, optionally bracketing its
    /// execution with a Darwin pthread QoS override matching the
    /// job's `priority`.
    ///
    /// - Parameters:
    ///   - job: The job to execute.
    ///   - executor: The unowned-executor identity to report.
    ///   - priorityTracking: If `true` and on Darwin, the current
    ///     thread's QoS is bumped to match `job.priority` for the
    ///     duration of the call, then reverted. If `false` or on
    ///     non-Darwin, the job runs without adjustment.
    internal static func runJob(
        _ job: UnownedJob,
        onSerial executor: UnownedSerialExecutor,
        priorityTracking: Bool
    ) {
        #if os(macOS) || os(iOS) || os(tvOS) || os(watchOS) || os(visionOS)
            if priorityTracking,
                let qos = Darwin.Kernel.Thread.QoS(priority: UInt32(job.priority.rawValue))
            {
                qos.withOverride {
                    unsafe job.runSynchronously(on: executor)
                }
            } else {
                unsafe job.runSynchronously(on: executor)
            }
        #else
            unsafe job.runSynchronously(on: executor)
        #endif
    }

    /// `UnownedTaskExecutor`-identity variant. See
    /// `runJob(_:onSerial:priorityTracking:)`.
    internal static func runJob(
        _ job: UnownedJob,
        onTask executor: UnownedTaskExecutor,
        priorityTracking: Bool
    ) {
        #if os(macOS) || os(iOS) || os(tvOS) || os(watchOS) || os(visionOS)
            if priorityTracking,
                let qos = Darwin.Kernel.Thread.QoS(priority: UInt32(job.priority.rawValue))
            {
                qos.withOverride {
                    unsafe job.runSynchronously(on: executor)
                }
            } else {
                unsafe job.runSynchronously(on: executor)
            }
        #else
            unsafe job.runSynchronously(on: executor)
        #endif
    }
}

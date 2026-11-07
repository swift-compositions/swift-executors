extension Kernel.Thread.Executor {

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

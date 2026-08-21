extension Kernel.Thread.Executor {

    public var runtime: Executor.Runtime {
        wait.withLock {
            if _loopExited { return .terminated }
            if _shutdown.isSet { return .draining }
            return .running
        }
    }
}

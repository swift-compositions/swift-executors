internal import Synchronizer_Blocking

extension Executor.Wait {

    public final class Condvar: Sendable {
        internal let sync: Synchronizer.Blocking<1>

        public init() {
            self.sync = .init()
        }
    }
}

extension Executor.Wait.Condvar {

    public func withLock<R, E: Swift.Error>(
        _ body: () throws(E) -> R
    ) throws(E) -> R {
        try sync.synchronize(body)
    }

    public func wait() {
        sync.wait()
    }

    public func wait(timeout: Duration) -> Bool {
        sync.wait(timeout: timeout)
    }
}

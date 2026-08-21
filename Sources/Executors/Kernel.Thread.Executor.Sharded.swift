import CPU_Primitives
import Index_Primitives
import Ordinal_Primitives
import Synchronization

extension Kernel.Thread.Executor {

    public final class Sharded: Sendable {

        internal let executors: [Kernel.Thread.Executor]
        public let count: Kernel.Thread.Count

        private let cursor: CPU.Cache.Padded<Atomic<Index<Kernel.Thread>>>

        public init(_ options: Options = .init()) {
            self.count = options.count
            let priorityTracking = options.priorityTracking
            self.executors = Array(count: options.count) { _ in
                Kernel.Thread.Executor(priorityTracking: priorityTracking)
            }
            self.cursor = .init(Atomic<Index<Kernel.Thread>>(.zero))
        }
    }
}

extension Kernel.Thread.Executor.Sharded {

    public func next() -> Kernel.Thread.Executor {
        executors[cursor.value.advance(within: count)]
    }

    public func executor(at index: Int) -> Kernel.Thread.Executor {
        executors[index % executors.count]
    }

    public func isIsolatingCurrentContext() -> Bool? {
        for executor in executors {
            if executor.isIsolatingCurrentContext() == true { return true }
        }
        return false
    }

    public func checkIsolated() {
        guard isIsolatingCurrentContext() == true else {
            preconditionFailure(
                "Kernel.Thread.Executor.Sharded: current thread is not a shard thread"
            )
        }
    }

    public func shutdown() {
        for executor in executors {
            executor.shutdown()
        }
    }
}

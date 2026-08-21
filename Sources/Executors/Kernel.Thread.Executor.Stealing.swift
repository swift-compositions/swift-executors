import Index_Primitives
import Ordinal_Primitives
import Synchronization

extension Kernel.Thread.Executor {

    public final class Stealing: TaskExecutor, @unsafe @unchecked Sendable {
        internal let workers: [Worker]
        internal let _shutdown: Executor_Primitives.Executor.Shutdown.Flag
        internal let priorityTracking: Bool
        private let cursor: Atomic<Index<Kernel.Thread>>
        public let count: Kernel.Thread.Count

        public init(_ options: Options = .init()) {
            self.count = options.count
            self._shutdown = .init()
            self.priorityTracking = options.priorityTracking
            self.cursor = .init(.zero)
            self.workers = Array(count: options.count) { position in
                Worker(id: position)
            }
            for worker in workers {
                worker.start(pool: self)
            }
        }
    }
}

extension Kernel.Thread.Executor.Stealing {
    public func enqueue(_ job: consuming ExecutorJob) {
        enqueue(UnownedJob(job))
    }

    public func enqueue(_ job: UnownedJob) {
        if workers[cursor.advance(within: count)].enqueue(job) { return }

        unsafe Kernel.Thread.Executor.runJob(
            job,
            onTask: asUnownedTaskExecutor(),
            priorityTracking: priorityTracking
        )
    }

    public func asUnownedTaskExecutor() -> UnownedTaskExecutor {
        unsafe UnownedTaskExecutor(ordinary: self)
    }
}

extension Kernel.Thread.Executor.Stealing {

    public func shutdown() {
        _shutdown.set()
        for worker in workers { worker.wake() }
        for worker in workers {

            do throws(Kernel.Thread.Error) {
                try worker.join()
            } catch {
            }
        }
    }
}

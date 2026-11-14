import Index
import Ordinal
import Synchronization
import Tagged

extension Kernel.Thread.Executor {

    public final class Stealing: TaskExecutor, @unsafe @unchecked Sendable {
        let crew: Crew
        private let cursor: Atomic<Index<Kernel.Thread>>
        public let count: Kernel.Thread.Count

        public init(_ options: Options = .init()) {
            self.count = options.count
            self.cursor = .init(.zero)
            let crew = Crew(
                workers: Array(count: options.count) { position in Worker(id: position) },
                count: options.count,
                priorityTracking: options.priorityTracking
            )
            self.crew = crew
            crew.pool = self
            for worker in crew.workers {
                worker.start(crew: crew)
            }
        }

        /// Ends the workers' threads once their queues are empty, without waiting for them.
        deinit {
            crew.end()
            for worker in crew.workers {
                worker.detach()
            }
        }
    }
}

extension Kernel.Thread.Executor.Stealing {
    public func enqueue(_ job: consuming ExecutorJob) {
        enqueue(UnownedJob(job))
    }

    public func enqueue(_ job: UnownedJob) {
        if crew.workers[cursor.advance(within: count)].enqueue(job) { return }
        run(job)
    }

    public func asUnownedTaskExecutor() -> UnownedTaskExecutor {
        unsafe UnownedTaskExecutor(ordinary: self)
    }

    /// Runs a job here, on this executor.
    func run(_ job: UnownedJob) {
        unsafe Kernel.Thread.Executor.runJob(
            job,
            onTask: asUnownedTaskExecutor(),
            priorityTracking: crew.priorityTracking
        )
    }
}

extension Kernel.Thread.Executor.Stealing {

    public func shutdown() {
        crew.end()
        for worker in crew.workers {

            do throws(Kernel.Thread.Error) {
                try worker.join()
            } catch {
            }
        }
    }
}

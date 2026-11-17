import Executors
import Synchronization
import Testing

enum `Executor.Timeout Tests` {}

extension `Executor.Timeout Tests` {
    /// Counts calls from any thread.
    final class Count: Sendable {
        let value = Atomic<Int>(0)
        func add() { value.add(1, ordering: .relaxed) }
        var total: Int { value.load(ordering: .relaxed) }
    }

    @Suite(.timeLimit(.minutes(1)))
    struct Test {
        @Test
        func `an answer that comes within the limit is what the wait gives, and the limit never expires`() async {
            let expired = Count()
            let answer = await Executor.Timeout.answer(within: .seconds(10), expired: { expired.add() }) { 42 }
            #expect(answer == 42)
            await Executor.Timeout.pause(.milliseconds(50))
            #expect(expired.total == 0)
        }

        /// A class that is not `Sendable`.
        final class Unshared {
            var value = 7
        }

        @Test
        func `an answer that is not Sendable is handed over whole`() async {
            let answer = await Executor.Timeout.answer(within: .seconds(10)) { Unshared() }
            #expect(answer?.value == 7)
        }

        @Test
        func `past the limit the wait gives nil and calls expired once, while the operation is left behind`() async {
            let expired = Count()
            let started = ContinuousClock.now
            let answer = await Executor.Timeout.answer(within: .milliseconds(100), expired: { expired.add() }) { () async -> Int in
                await Executor.Timeout.pause(.seconds(5))
                return 1
            }
            #expect(answer == nil)
            #expect(expired.total == 1)
            #expect(ContinuousClock.now - started < .seconds(4))
        }

        @Test
        func `a limit of no time ends the wait at once and starts nothing`() async {
            let (expired, started) = (Count(), Count())
            let answer = await Executor.Timeout.answer(within: .zero, expired: { expired.add() }) { () async -> Int in
                started.add()
                return 1
            }
            await Executor.Timeout.pause(.milliseconds(50))
            #expect(answer == nil)
            #expect(expired.total == 1)
            #expect(started.total == 0)
        }

        @Test
        func `a cancelled wait ends at once with nil and does not call expired`() async {
            let expired = Count()
            let waiting = Task {
                await Executor.Timeout.answer(within: .seconds(30), expired: { expired.add() }) { () async -> Int in
                    await Executor.Timeout.pause(.seconds(5))
                    return 1
                }
            }
            await Executor.Timeout.pause(.milliseconds(50))
            waiting.cancel()
            #expect(await waiting.value == nil)
            #expect(expired.total == 0)
        }

        @Test
        func `an operation that blocks the thread of its executor does not hold the limit up`() async {
            let executor = Kernel.Thread.Executor()
            let answer = await Executor.Timeout.answer(within: .milliseconds(100), on: executor) { () -> Int in
                Self.block(milliseconds: 1_000)
                return 1
            }
            #expect(answer == nil)
        }

        nonisolated static func block(milliseconds: Int) {
            let until = ContinuousClock.now + .milliseconds(milliseconds)
            while ContinuousClock.now < until {}
        }

        @Test
        func `a timer fires once after its time, and cancelling it first stops it`() async {
            let (fired, stopped) = (Count(), Count())
            let firing = Executor.Timeout.Timer(after: .milliseconds(20)) { fired.add() }
            let stopping = Executor.Timeout.Timer(after: .milliseconds(20)) { stopped.add() }
            #expect(stopping.cancel())
            await Executor.Timeout.pause(.milliseconds(200))
            #expect(fired.total == 1 && firing.fired)
            #expect(!firing.cancel())
            #expect(stopped.total == 0 && !stopping.fired)
            #expect(!stopping.cancel())
        }

        @Test
        func `a pause lasts its time, no time is no wait, and a cancelled pause ends at once`() async {
            var started = ContinuousClock.now
            await Executor.Timeout.pause(.milliseconds(100))
            #expect(ContinuousClock.now - started >= .milliseconds(90))
            started = ContinuousClock.now
            await Executor.Timeout.pause(.zero)
            #expect(ContinuousClock.now - started < .milliseconds(50))
            let pausing = Task { await Executor.Timeout.pause(.seconds(30)) }
            started = ContinuousClock.now
            await Executor.Timeout.pause(.milliseconds(20))
            pausing.cancel()
            await pausing.value
            #expect(ContinuousClock.now - started < .seconds(5))
        }
    }
}

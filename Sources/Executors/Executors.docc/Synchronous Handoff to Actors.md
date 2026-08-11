# Synchronous Handoff to Actors

@Metadata {
    @DisplayName("Synchronous Handoff to Actors")
    @TitleHeading("Pattern")
}

Delivering fire-and-forget work to an actor from a non-async, non-isolated
caller — without per-message `Task` allocation and without losing FIFO
ordering.

## Problem

A synchronous, non-isolated caller — a signal handler, a kqueue/epoll
callback, an IO completion handler, an FFI trampoline, a blocking-thread
bridge — has work to deliver to an actor pinned to one of the executors in
this module. The work is fire-and-forget: the caller does not await a
result, does not need cancellation per message, and does not care about
priority.

Four mechanisms appear to fit:

| Mechanism | What it is |
|-----------|------------|
| `Task { await actor.method(work) }` | Language-level fire-and-forget |
| `Task.immediate { await actor.method(work) }` | SE-0472. Body runs synchronously up to the first suspension |
| Direct `executor.enqueue(_ job: UnownedJob)` | The public sync entry point on every executor in this module |
| Channel-based handoff | `Async.Channel.Unbounded.Sender.send(_:)` from sync code; actor owns the consumer |

This article explains why **channel-based handoff** is the recommended
pattern, and why the obvious alternatives are subordinate.

## Recommended Pattern

The actor owns an `Async.Channel.Unbounded` receiver and consumes it in a
`for await` loop on its own executor. The matching `Sender` is `Sendable`
and is exposed to sync callers.

```swift
import Async
import Executors

public actor Worker {

    public struct Sink: Sendable {
        let sender: Async.Channel<Work>.Unbounded.Sender

        public func send(_ work: Work) {
            try? sender.send(work)
        }
    }

    private let executor: Kernel.Thread.Executor.Polling
    private var channel: Async.Channel<Work>.Unbounded

    public init(executor: Kernel.Thread.Executor.Polling) {
        self.executor = executor
        self.channel = Async.Channel<Work>.Unbounded()
    }

    public nonisolated var unownedExecutor: UnownedSerialExecutor {
        executor.asUnownedSerialExecutor()
    }

    public func sink() -> Sink {
        Sink(sender: channel.sender)
    }

    public func run() async {
        let ends = (consume channel).take().ends()
        for await work in ends.receiver {
            handle(work)
        }
    }

    private func handle(_ work: Work) {
        // ...
    }
}
```

A sync caller obtains the `Sink` once and sends from any context:

```swift
let worker = Worker(executor: pollingExecutor)
let sink = await worker.sink()
Task { await worker.run() }

// From any sync context — no await, no Task per message:
sink.send(work)
```

## Why Other Patterns Are Subordinate

### `Task { await actor.method(work) }`

Two scheduling hops per message: the new `Task` enqueues onto the global
concurrent executor, runs there briefly, hits the `await`, then enqueues
onto the actor. Allocates a task record per message. Does not preserve
arrival order across concurrent enqueues — the actor sees messages in
whatever order the runtime delivers them. Never the right answer when the
caller is non-isolated and non-async.

### `Task.immediate { await actor.method(work) }`

One scheduling hop per message — the body runs synchronously until the
actor hop, skipping the initial executor bounce. Still allocates a task
record per message. Still does not preserve FIFO ordering across
concurrent enqueues. Appropriate when each message genuinely needs its
own cancellation token, priority, or task-local state — i.e., when the
work is "fire a structured task and forget the handle." Inappropriate
when the work is plain fire-and-forget.

### Direct `executor.enqueue(_ job: UnownedJob)`

Every executor in this module exposes a public synchronous
`enqueue(_:UnownedJob)`. It looks like the answer — and is not. The
parameter is `UnownedJob`, which has no public constructor accepting a
closure. The method is a forwarding entry point used by the runtime to
deliver jobs the runtime itself constructs (most commonly on behalf of
`Task` and `Task.immediate`). User code cannot synthesize an `UnownedJob`
from arbitrary work; any wrapper that tried to would internally allocate
a `Task`, defeating the purpose.

The `enqueue(_:)` surface remains useful for executor composition (one
executor forwarding jobs to another) and for the runtime's own
scheduling. It is not a user-level work-submission API.

### Custom executor with an internal sync queue

Always available: implement a `SerialExecutor` whose `enqueue(_:)` drains
both runtime-supplied jobs and a separate internal queue of closures
populated by sync callers. More invasive than the channel pattern, but
the right answer when the actor needs delivery semantics that a plain
channel cannot express — priority dispatch, deduplication, coalescing,
or wakeup-coalescing across sources.

## Why Channel-Based Wins

| Property | `Task` | `Task.immediate` | Direct `enqueue` | Channel |
|----------|:---:|:---:|:---:|:---:|
| Sync from non-async caller | yes | yes | yes (but unusable) | yes |
| Per-message `Task` allocation | yes | yes | n/a | no |
| FIFO ordering | no | no | n/a | yes |
| Backpressure available | no | no | no | yes (`Bounded`) |
| Scheduling hops | 2 | 1 | n/a | 1 |
| User-constructible work | yes | yes | no | yes |

The channel pattern allocates the channel once at construction, then per
message pays only the channel-send cost. The consumer loop runs on the
actor's executor — there is no hop on the receiving side because the
consumer is already there. Switching `Unbounded` for `Bounded` adds
backpressure with no other code changes.

## Precedent in This Ecosystem

The `swift-io` package commits to the same `Async.Channel` primitive for
a related case. `IO.Event.Actor` runs its tick closure on its
`Kernel.Thread.Executor.Polling` thread under `assumeIsolated` and
broadcasts kernel events to per-call
`Async.Channel<Kernel.Event>.Unbounded` senders held in the
registration table. The direction is owned-executor → awaiter rather
than external-thread → actor, but the primitive and the avoidance of
per-message `Task` allocation are the same. See
`IO.Event.Actor.wait(for:interest:)` for the construction site.

## Shutdown

Channel close terminates the consumer loop: when the sender side is
dropped or explicitly closed, the receiver yields `nil` and the `for
await` loop exits. The actor's `run()` method returns, mirroring the
existing shutdown pattern in `IO.Event.Actor`.

## Research

- [Synchronous Handoff to Actors](https://github.com/swift-institute/Research/blob/main/Packages/swift-executors/sync-handoff-to-actors.md) — Trade-off analysis for the four available mechanisms. Status: DECISION.

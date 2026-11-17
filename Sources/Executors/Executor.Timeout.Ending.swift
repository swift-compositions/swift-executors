#if canImport(Dispatch)
    import Synchronization

    extension Executor.Timeout {
        /// The one end of a wait: its answer, or nil for its limit or a cancellation, whichever comes first; the
        /// others are dropped. An end that comes before the waiter is kept for it.
        final class Ending<Answer: Sendable>: Sendable {
            let state = Mutex<State>(State())
        }
    }

    extension Executor.Timeout.Ending {
        /// Waits through `continuation` for the end, or resumes it at once with an end that came before.
        func begin(_ continuation: CheckedContinuation<Answer?, Never>) {
            let early = state.withLock { state -> Answer?? in
                guard let ended = state.ended else {
                    state.waiting = continuation
                    return nil
                }
                return ended
            }
            if let early { continuation.resume(returning: early) }
        }

        /// Ends the wait with `answer`: true when this was its end, false when it had already ended.
        func end(with answer: Answer?) -> Bool {
            let (first, waiting) = state.withLock { state -> (Bool, CheckedContinuation<Answer?, Never>?) in
                guard state.ended == nil else { return (false, nil) }
                state.ended = .some(answer)
                defer { state.waiting = nil }
                return (true, state.waiting)
            }
            waiting?.resume(returning: answer)
            return first
        }
    }
#endif

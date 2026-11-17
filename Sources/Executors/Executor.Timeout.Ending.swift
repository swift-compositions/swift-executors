#if canImport(Dispatch)
    import Synchronization

    extension Executor.Timeout {
        /// The one end of a wait: its answer, or nil for its limit or a cancellation, whichever comes first; the
        /// others are dropped. An end that comes before the waiter is kept for it.
        final class Ending<Answer>: Sendable {
            let state = Mutex<State>(State())
        }
    }

    extension Executor.Timeout.Ending {
        /// Waits through `continuation` for the end, or resumes it at once with an end that came before.
        func begin(_ continuation: CheckedContinuation<Answer?, Never>) {
            let early = state.withLock { state -> Parcel? in
                guard let ended = state.ended else {
                    state.waiting = continuation
                    return nil
                }
                state.ended = nil
                return ended
            }
            if let early { continuation.resume(returning: early.answer) }
        }

        /// Ends the wait with `answer`: true when this was its end, false when it had already ended.
        func end(with answer: sending Answer?) -> Bool {
            let parcel = Parcel(answer: answer)
            let (first, waiting) = state.withLock { state -> (Bool, CheckedContinuation<Answer?, Never>?) in
                guard !state.done else { return (false, nil) }
                state.done = true
                guard let waiting = state.waiting else {
                    state.ended = parcel
                    return (true, nil)
                }
                state.waiting = nil
                return (true, waiting)
            }
            waiting?.resume(returning: parcel.answer)
            return first
        }
    }
#endif

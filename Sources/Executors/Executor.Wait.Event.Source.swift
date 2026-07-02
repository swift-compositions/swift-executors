// ===----------------------------------------------------------------------===//
//
// This source file is part of the swift-primitives open source project
//
// Copyright (c) 2024-2026 Coen ten Thije Boonkkamp and the swift-primitives
// project authors. Licensed under Apache License v2.0
//
// See LICENSE for license information
//
// ===----------------------------------------------------------------------===//

// Windows has no Kernel.Event.Source (epoll/kqueue vocabulary; the target
// is !os(Windows)); this wrapper and its only consumer (Kernel.Thread
// .Executor.Polling) gate together. The IOCP analog arrives with the
// completion-port wait primitive.
#if !os(Windows)

public import Executor_Primitives
public import Kernel


extension Executor.Wait.Event {
    /// Wait primitive backed by a kernel event source.
    ///
    /// Holds the `~Copyable` source; exposes `wakeup` (`Sendable`) for cross-
    /// thread signaling. `wait` polls the source with no lock held. Consumers
    /// handle the returned events — this type is transport, not dispatch.
    ///
    /// ## Wait.Primitive contract
    ///
    /// Satisfies the conceptual Wait.Primitive contract statically. When a
    /// third wait mechanism ships, `Wait.Primitive` becomes a real protocol
    /// — each existing type gains a retroactive conformance via typealias
    /// bridge (non-breaking).
    public struct Source: ~Copyable {
        @usableFromInline
        internal var _source: Kernel.Event.Source

        /// Thread-safe wakeup channel for interrupting a blocking `wait`.
        public let wakeup: Kernel.Wakeup.Channel

        @inlinable
        public init(source: consuming Kernel.Event.Source) {
            self.wakeup = source.wakeup
            self._source = consume source
        }

        deinit {
            _source.close()
        }
    }
}

extension Executor.Wait.Event.Source {
    /// Block until an event arrives or wakeup fires.
    ///
    /// - Parameters:
    ///   - deadline: Timeout deadline, or `nil` for indefinite wait.
    ///   - buffer: Output buffer for received events.
    /// - Returns: The number of events written into `buffer`.
    @inlinable
    public mutating func wait(
        deadline: Clock.Continuous.Deadline?,
        into buffer: inout [Kernel.Event]
    ) throws(Kernel.Event.Driver.Error) -> Int {
        try _source.poll(deadline: deadline, into: &buffer)
    }

    /// Direct access to the underlying event source for registration
    /// and configuration. Coroutine-scoped — the reference cannot escape.
    @inlinable
    public var source: Kernel.Event.Source {
        _read { yield _source }
        _modify { yield &_source }
    }
}


#endif

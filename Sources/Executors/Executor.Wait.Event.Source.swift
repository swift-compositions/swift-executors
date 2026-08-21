#if !os(Windows)

    public import Executor_Primitives
    public import Kernel

    extension Executor.Wait.Event {

        public struct Source: ~Copyable {
            @usableFromInline
            internal var _source: Kernel.Event.Source

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

        @inlinable
        public mutating func wait(
            deadline: Clock.Continuous.Deadline?,
            into buffer: inout [Kernel.Event]
        ) throws(Kernel.Event.Driver.Error) -> Int {
            try _source.poll(deadline: deadline, into: &buffer)
        }

        @inlinable
        public var source: Kernel.Event.Source {
            _read { yield _source }
            _modify { yield &_source }
        }
    }

#endif

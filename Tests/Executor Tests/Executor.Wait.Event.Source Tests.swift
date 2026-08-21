import Executors
import Testing

#if !os(Windows)

    extension Executor.Wait.Event.Source {
        @Suite
        struct Test {

            @Test
            func `Wait.Event namespace exists`() {

                _ = Executor.Wait.Event.self
            }
        }
    }

#endif

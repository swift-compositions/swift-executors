import Executors
import Testing

enum `Executor.Main Tests` {}

extension `Executor.Main Tests` {
    @Suite
    struct Test {
        @Test
        func `Main.shared returns an identity`() {
            let main = Executor.Main.shared
            _ = unsafe main.asUnownedSerialExecutor()
        }
    }
}

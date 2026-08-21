import Executors
import Testing

extension Executor.Main {
    @Suite
    struct Test {
        @Test
        func `Main.shared returns an identity`() {
            let main = Executor.Main.shared
            _ = main.asUnownedSerialExecutor()
        }
    }
}

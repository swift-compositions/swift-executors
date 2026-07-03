//
//  Executor.Main Tests.swift
//  swift-executors
//

import Executors
import Testing

@Suite
struct MainTests {
    @Test
    func `Main.shared returns an identity`() {
        let main = Executor.Main.shared
        _ = main.asUnownedSerialExecutor()
    }
}

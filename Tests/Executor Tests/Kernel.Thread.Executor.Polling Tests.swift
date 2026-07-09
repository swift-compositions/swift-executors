//
//  Kernel.Thread.Executor.Polling Tests.swift
//  swift-executors
//

import Executors
import Testing

// Polling requires a Kernel.Event.Source which needs platform infrastructure.
// Lifecycle and behavioral tests are provided by swift-io (Phase 3).
// This file serves as a compile-validation placeholder.

#if !os(Windows)

    extension Kernel.Thread.Executor.Polling {
        @Suite
        struct Test {
            @Test
            func `Polling.Outcome enum exists`() {
                _ = Kernel.Thread.Executor.Polling.Outcome.continue
                _ = Kernel.Thread.Executor.Polling.Outcome.halt
            }
        }
    }

#endif

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

import Executors
import Testing

// Executor.Wait.Event.Source requires a Kernel.Event.Driver (L2/L3) to
// construct. No test factory exists at L1. Lifecycle and behavioral
// coverage is provided by the L3 integration tests in swift-executors
// (Phase 2) and swift-io (Phase 3).
//
// This file validates type availability and namespace structure.

// Mirrors the guard on Sources/Executors/Executor.Wait.Event.Source.swift:
// Windows has no Kernel.Event.Source (epoll/kqueue vocabulary), so the type
// under test does not exist there and this suite cannot compile. The sibling
// Kernel.Thread.Executor.Polling Tests.swift gates the same way. Remove this
// guard when the IOCP-backed completion-port wait primitive lands.
#if !os(Windows)

    extension Executor.Wait.Event.Source {
        @Suite
        struct Test {

            @Test
            func `Wait.Event namespace exists`() {
                // Compile-time validation: the namespace enum is reachable.
                _ = Executor.Wait.Event.self
            }
        }
    }

#endif

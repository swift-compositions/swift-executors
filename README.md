# swift-executors

![Development Status](https://img.shields.io/badge/status-active--development-blue.svg)

Executor composition for Swift. Provides sharded executor pools for parallel work dispatch using `Kernel.Thread.Executor` instances from swift-kernel. Layer 3 (Foundations) of the Swift Institute five-layer architecture.

---

## Key Features

- **Sharded executor pools** -- Round-robin distribution across N serial executors
- **Typed throws** -- No `any Error` at the API surface
- **Foundation-free** -- No Foundation module dependencies
- **Swift 6 strict concurrency** -- Full `Sendable` compliance

---

## Installation

### Package.swift dependency

```swift
dependencies: [
    .package(url: "https://github.com/swift-foundations/swift-executors.git", branch: "main")
]
```

> Pre-1.0: no version tags yet. APIs may change; pin a commit for reproducible builds.

### Target dependency

```swift
.target(
    name: "YourTarget",
    dependencies: [
        .product(name: "Executors", package: "swift-executors")
    ]
)
```

### Requirements

- Swift 6.2+
- macOS 26.0+ / iOS 26.0+ / tvOS 26.0+ / watchOS 26.0+
- Linux (Ubuntu 22.04+)
- Windows (Swift 6.2+)

---

## Quick Start

```swift
import Executors

let pool = Kernel.Thread.Executor.Sharded(.init(count: 4))
defer { pool.shutdown() }

let executor = pool.next()
// Use executor for task dispatch or actor pinning
```

---

## Key Types

| Type | Purpose |
|------|---------|
| `Kernel.Thread.Executor.Sharded` | Sharded pool of serial executors with round-robin routing |
| `Kernel.Thread.Executor.Sharded.Options` | Configuration for thread count |

---

## Architecture

`swift-executors` sits at Layer 3 (Foundations), depending on `swift-kernel` for `Kernel.Thread.Executor`.

```
swift-executors     <-- Executor composition (this package)
     |
swift-kernel        <-- Serial executor, OS thread primitives
     |
swift-kernel-primitives  <-- Syscall vocabulary
```

---

## Related Packages

### Dependencies

- [swift-kernel](https://github.com/swift-foundations/swift-kernel): Serial executors backed by dedicated OS threads

### Used By

- [swift-io](https://github.com/swift-foundations/swift-io): Async I/O executor with typed throws

---

## License

Apache 2.0 -- See [LICENSE](LICENSE.md) for details.

// swift-tools-version: 6.4

import PackageDescription

let package = Package(
    name: "swift-executors",
    platforms: [
        .macOS(.v27),
        .iOS(.v27),
        .tvOS(.v27),
        .watchOS(.v27),
        .visionOS(.v27),
    ],
    products: [
        .library(name: "Executors", targets: ["Executors"])
    ],
    dependencies: [
        .package(url: "https://github.com/swift-atoms/swift-tagged.git", branch: "main"),
        .package(url: "https://github.com/swift-compositions/swift-kernel.git", branch: "main"),
        .package(
            url: "https://github.com/swift-compositions/swift-synchronizers.git",
            branch: "main"
        ),
        .package(
            url: "https://github.com/swift-atoms/swift-executor.git",
            branch: "main"
        ),
        .package(
            url: "https://github.com/swift-atoms/swift-property.git",
            branch: "main"
        ),
        .package(
            url: "https://github.com/swift-atoms/swift-ordinal.git",
            branch: "main"
        ),
        .package(
            url: "https://github.com/swift-atoms/swift-index.git",
            branch: "main"
        ),
        .package(
            url: "https://github.com/swift-atoms/swift-cpu.git",
            branch: "main"
        ),
        .package(url: "https://github.com/swift-atoms/swift-system.git", branch: "main"),
    ],
    targets: [
        .target(
            name: "Executors",
            dependencies: [
                .product(name: "Tagged", package: "swift-tagged"),
                .product(name: "Kernel", package: "swift-kernel"),
                .product(name: "Synchronizer Blocking", package: "swift-synchronizers"),
                .product(name: "Executor", package: "swift-executor"),
                .product(name: "Property", package: "swift-property"),
                .product(name: "Ordinal", package: "swift-ordinal"),
                .product(name: "Index", package: "swift-index"),
                .product(name: "CPU", package: "swift-cpu"),
                .product(name: "System Primitives", package: "swift-system"),
            ]
        ),
        .testTarget(
            name: "Executors Tests",
            dependencies: [
                "Executors",
                .product(name: "Kernel Test Support", package: "swift-kernel"),
            ],
            path: "Tests/Executor Tests"
        ),
    ],
    swiftLanguageModes: [.v6]
)

for target in package.targets where ![.system, .binary, .plugin, .macro].contains(target.type) {
    let ecosystem: [SwiftSetting] = [
        .strictMemorySafety(),
        .enableUpcomingFeature("ExistentialAny"),
        .enableUpcomingFeature("InternalImportsByDefault"),
        .enableUpcomingFeature("MemberImportVisibility"),
        .enableUpcomingFeature("NonisolatedNonsendingByDefault"),
        .enableExperimentalFeature("Lifetimes"),
        .enableUpcomingFeature("InferIsolatedConformances"),
    ]

    let package: [SwiftSetting] = []

    target.swiftSettings = (target.swiftSettings ?? []) + ecosystem + package
}

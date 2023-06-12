// swift-tools-version: 5.8
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "SwiftArrowParquet",
    platforms: [
      .macOS(.v13)
    ],
    products: [
        // Products define the executables and libraries a package produces, making them visible to other packages.
        .library(
            name: "SwiftArrowParquet",
            targets: ["SwiftArrowParquet"]),
    ],
    targets: [
        // Targets are the basic building blocks of a package, defining a module or a test suite.
        // Targets can depend on other targets in this package and products from dependencies.
        .target(
            name: "SwiftArrowParquet",
            dependencies: ["CApacheArrowGlib", "CApacheParquetGlib"]
        ),
        .systemLibrary(
            name: "CApacheArrowGlib",
            pkgConfig: "arrow-glib",
            providers: [.brew(["apache-arrow-glib"]), .apt(["libarrow-glib-dev"])]
        ),
        .systemLibrary(
            name: "CApacheParquetGlib",
            pkgConfig: "parquet-glib",
            providers: [.brew(["apache-arrow-glib"]), .apt(["libparquet-glib-dev"])]
        ),
        .testTarget(
            name: "SwiftArrowParquetTests",
            dependencies: ["SwiftArrowParquet"]),
    ]
)

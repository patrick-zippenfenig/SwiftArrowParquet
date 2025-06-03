# SwiftArrowParquet

Rudimentary Swift Arrow Parquet Wrapper for the Apache Arrow GLIB library.

WARNING: Skeleton code mostly and limited funktionality, but can be extended easily. Writing a simple Parquet file is currently the only functionality

## Usage

1. Install the Apache Arrow GLIB library via `brew install apache-arrow-glib` or apt:

```bash
sudo apt update
sudo apt install -y -V ca-certificates lsb-release wget
wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt update
sudo apt install -y -V libarrow-glib-dev libparquet-glib-dev
```

See https://arrow.apache.org/install/ for more installations instructions

2. Add SwiftArrowParquet as a dependency to your Package.swift

```
  dependencies: [
    .package(url: "https://github.com/patrick-zippenfenig/SwiftArrowParquet.git", from: "1.0.0")
  ],
  targets: [
    .target(name: "MyApp", dependencies: [
      .product(name: "SwiftArrowParquet", package: "SwiftArrowParquet"),
    ])
  ]
  ```


3. Import SwiftArrowParquet and use it.

```swift
import SwiftArrowParquet

let schema = try ArrowSchema([
    ("id", .int64),
    ("data", .float),
    ("time", .timestamp(unit: .second))
])

let ids = try ArrowArray(int64: [1,2,3,4,5])
let data = try ArrowArray(float: [1,2,3,4,5])
let time = try ArrowArray(timestamp: [123123,12312312,12312312,12312312,12312312], unit: .second)

let table = try ArrowTable(schema: schema, arrays: [ids, data, time])

let properties = ParquetWriterProperties()
properties.setCompression(type: .lz4, path: "id")
properties.setCompression(type: .lz4, path: "data")
properties.setCompression(type: .lz4, path: "time")
let writer = try ParquetFileWriter(path: "./test.pq", schema: schema, properties: properties)
try writer.write(table: table, chunkSize: 5)
try writer.write(table: table, chunkSize: 5)
try writer.close()
```

import XCTest
/*@testable*/ import SwiftArrowParquet

final class SwiftArrowParquetTests: XCTestCase {
    func testExample() throws {
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
    }
}

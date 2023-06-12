import XCTest
@testable import SwiftArrowParquet

final class SwiftArrowParquetTests: XCTestCase {
    func testExample() throws {
        let array = try ArrowArray([1,2,3,4,5])
        let schema = try ArrowSchema(["test": array])
        
        let table = try ArrowTable(schema: schema, arrays: [array])
        
        let properties = ParquetWriterProperties()
        properties.setCompression(type: .lz4, path: ".")
        let writer = try ParquetFileWriter(path: "/Users/patrick/Downloads/test.pq", schema: schema, properties: properties)
        try writer.write(table: table)
        try writer.write(table: table)
        try writer.close()
    }
}

import XCTest
@testable import SwiftArrowParquet

final class SwiftArrowParquetTests: XCTestCase {
    func testExample() throws {
        let array = ArrowArray([1,2,3,4,5])
        let schema = ArrowSchema(["test": array])
        
        let table = ArrowTable(schema: schema, arrays: [array])
        
        let properties = ParquetWriterProperties()
        properties.setCompression(type: .lz4, path: ".")
        let writer = ParquetFileWriter(path: "/Users/patrick/Downloads/test.pq", schema: schema, properties: properties)
        writer.write(table: table)
        writer.write(table: table)
    }
}

import Testing
/*@testable*/ import SwiftArrowParquet

struct SwiftArrowParquetTests {
    @Test func testExample() throws {
        let schema = try ArrowSchema([
            ("id", .int64),
            ("data", .float),
            ("time", .timestamp(unit: .second))
        ])
        
        #expect(schema.toString() == """
                       id: int64
                       data: float
                       time: timestamp[s]
                       """)
        
        #expect(schema.countFields == 3)
        
        let ids = try ArrowArray(int64: [1,2,3,4,5])
        let data = try ArrowArray(float: [1,2,3,4,5])
        let time = try ArrowArray(timestamp: [123123,12312312,12312312,12312312,12312312], unit: .second)
        
        #expect(ids.count == 5)
        #expect(data.count == 5)
        #expect(time.count == 5)
        
        #expect(try ids.toString() == "[\n  1,\n  2,\n  3,\n  4,\n  5\n]")
        #expect(try data.toString() == "[\n  1,\n  2,\n  3,\n  4,\n  5\n]")
        #expect(try time.toString() == """
            [
              1970-01-02 10:12:03,
              1970-05-23 12:05:12,
              1970-05-23 12:05:12,
              1970-05-23 12:05:12,
              1970-05-23 12:05:12
            ]
            """)
        
        let table = try ArrowTable(schema: schema, arrays: [ids, data, time])
        #expect(try table.toString() == """
                       id: int64
                       data: float
                       time: timestamp[s]
                       ----
                       id:
                         [
                           [
                             1,
                             2,
                             3,
                             4,
                             5
                           ]
                         ]
                       data:
                         [
                           [
                             1,
                             2,
                             3,
                             4,
                             5
                           ]
                         ]
                       time:
                         [
                           [
                             1970-01-02 10:12:03,
                             1970-05-23 12:05:12,
                             1970-05-23 12:05:12,
                             1970-05-23 12:05:12,
                             1970-05-23 12:05:12
                           ]
                         ]\n
                       """)
        
        #expect(table.rowCount == 5)
        #expect(table.columnCount == 3)
        
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

import CApacheArrowGlib
import CApacheParquetGlib

// The Swift Programming Language
// https://docs.swift.org/swift-book

struct Table {
    
}


public func test() {
    let writerProperties = gparquet_writer_properties_new()
    
    var arrays: [UnsafeMutablePointer<GArrowArray>?] = [Float.toGArrowArray(array: [1,2,3,4,5])]
    let columns = ["test"]
    
    var fields: UnsafeMutablePointer<GList>?
    for (array, column) in zip(arrays, columns) {
        let dataType = garrow_array_get_value_data_type(array)
        let field = garrow_field_new(column, dataType)
        fields = g_list_prepend(fields, field)
    }
    fields = g_list_reverse(fields)
    let schema = garrow_schema_new(fields)
    defer { g_object_unref(schema) }
    
    var error: UnsafeMutablePointer<GError>? = nil
    
    guard let table = garrow_table_new_arrays(schema, &arrays, UInt(arrays.count), &error) else {
        let errorString: String = String(cString: error!.pointee.message)
        g_error_free(error)
        fatalError(errorString)
    }
    
    let writer = gparquet_arrow_file_writer_new_path(schema, "/Users/patrick/Downloads/test.pq", writerProperties, &error)
    
    guard gparquet_arrow_file_writer_write_table(writer, table, 10, &error) != 0 else {
        fatalError()
    }
    
    guard gparquet_arrow_file_writer_close(writer, &error) != 0 else {
        fatalError()
    }
}


extension Float {
    public static func toGArrowArray(array: [Float]) -> UnsafeMutablePointer<GArrowArray>? {
        var error: UnsafeMutablePointer<GError>?
        let arrayBuilder = garrow_float_array_builder_new()
        defer { g_object_unref(arrayBuilder) }
        let numValues: Int64 = Int64(array.count)
        array.withUnsafeBufferPointer( { ptr in
            guard garrow_float_array_builder_append_values(arrayBuilder, ptr.baseAddress, numValues, [], 0, &error) != 0 else {
                let errorString: String = error != nil ? String(cString: error!.pointee.message) : ""
                g_error_free(error)
                fatalError(errorString)
            }
        })

        guard let garray = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(arrayBuilder), &error) else {
            let errorString: String = String(cString: error!.pointee.message)
            g_error_free(error)
            fatalError(errorString)
        }
        return garray
    }
}

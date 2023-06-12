import CApacheArrowGlib
import CApacheParquetGlib


final class ArrowSchema {
    let ptr: UnsafeMutablePointer<GArrowSchema>
                                    
    public init(_ columns: [String: ArrowArray]) {
        var fields: UnsafeMutablePointer<GList>?
        for (column, array) in columns {
            let dataType = garrow_array_get_value_data_type(array.ptr)
            let field = garrow_field_new(column, dataType)
            fields = g_list_prepend(fields, field)
        }
        fields = g_list_reverse(fields)
        guard let schema = garrow_schema_new(fields) else {
            fatalError()
        }
        ptr = schema
    }
    
    deinit {
        g_object_unref(ptr)
    }
}


final class ArrowTable {
    let ptr: UnsafeMutablePointer<GArrowTable>
    
    public init(schema: ArrowSchema, arrays: [ArrowArray]) {
        var error: UnsafeMutablePointer<GError>? = nil
        var arrayPtr: [UnsafeMutablePointer<GArrowArray>?] = arrays.map { $0.ptr }
        
        guard let table = garrow_table_new_arrays(schema.ptr, &arrayPtr, UInt(arrays.count), &error) else {
            let errorString: String = String(cString: error!.pointee.message)
            g_error_free(error)
            fatalError(errorString)
        }
        ptr = table
    }
    
    deinit {
        g_object_unref(ptr)
    }
}


final class ArrowArray {
    let ptr: UnsafeMutablePointer<GArrowArray>
    
    public init(_ array: [Float]) {
        var error: UnsafeMutablePointer<GError>?
        let arrayBuilder = garrow_float_array_builder_new()
        defer { g_object_unref(arrayBuilder) }
        array.withUnsafeBufferPointer( { ptr in
            guard garrow_float_array_builder_append_values(arrayBuilder, ptr.baseAddress, gint64(ptr.count), [], 0, &error) != 0 else {
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
        ptr = garray
    }
    
    deinit {
        g_object_unref(ptr)
    }
}


final class ParquetFileWriter {
    let ptr: UnsafeMutablePointer<GParquetArrowFileWriter>
    
    public init(path: String, schema: ArrowSchema) {
        let writerProperties = gparquet_writer_properties_new()
        defer { g_object_unref(writerProperties) }
        var error: UnsafeMutablePointer<GError>? = nil
        guard let writer = gparquet_arrow_file_writer_new_path(schema.ptr, path, writerProperties, &error) else {
            let errorString: String = String(cString: error!.pointee.message)
            g_error_free(error)
            fatalError(errorString)
        }
        ptr = writer
    }
    
    public func write(table: ArrowTable) {
        var error: UnsafeMutablePointer<GError>? = nil
        guard gparquet_arrow_file_writer_write_table(ptr, table.ptr, 10, &error) != 0 else {
            fatalError()
        }
    }
    
    deinit {
        var error: UnsafeMutablePointer<GError>? = nil
        guard gparquet_arrow_file_writer_close(ptr, &error) != 0 else {
            let errorString: String = String(cString: error!.pointee.message)
            g_error_free(error)
            fatalError(errorString)
        }
        g_object_unref(ptr)
    }
}

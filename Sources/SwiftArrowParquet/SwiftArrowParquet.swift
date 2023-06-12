import CApacheArrowGlib
import CApacheParquetGlib


enum ArrowError: Error {
    case schemaError
    case tableError(message: String)
    case arrayError(message: String)
    case fileWriterError(message: String)
}

public final class ArrowSchema {
    let ptr: UnsafeMutablePointer<GArrowSchema>
                                    
    public init(_ columns: [String: ArrowArray]) throws {
        var fields: UnsafeMutablePointer<GList>?
        for (column, array) in columns {
            let dataType = garrow_array_get_value_data_type(array.ptr)
            let field = garrow_field_new(column, dataType)
            fields = g_list_prepend(fields, field)
        }
        fields = g_list_reverse(fields)
        guard let schema = garrow_schema_new(fields) else {
            throw ArrowError.schemaError
        }
        ptr = schema
    }
    
    deinit {
        g_object_unref(ptr)
    }
}


public final class ArrowTable {
    let ptr: UnsafeMutablePointer<GArrowTable>
    
    public init(schema: ArrowSchema, arrays: [ArrowArray]) throws {
        var error: UnsafeMutablePointer<GError>? = nil
        var arrayPtr: [UnsafeMutablePointer<GArrowArray>?] = arrays.map { $0.ptr }
        
        guard let table = garrow_table_new_arrays(schema.ptr, &arrayPtr, UInt(arrays.count), &error) else {
            defer { g_error_free(error)}
            throw ArrowError.tableError(message: error.map {String(cString: $0.pointee.message) } ?? "")
        }
        ptr = table
    }
    
    deinit {
        g_object_unref(ptr)
    }
}


public final class ArrowArray {
    let ptr: UnsafeMutablePointer<GArrowArray>
    
    public init(_ array: [Float]) throws {
        var error: UnsafeMutablePointer<GError>?
        let arrayBuilder = garrow_float_array_builder_new()
        defer { g_object_unref(arrayBuilder) }
        try array.withUnsafeBufferPointer( { ptr in
            guard garrow_float_array_builder_append_values(arrayBuilder, ptr.baseAddress, gint64(ptr.count), [], 0, &error) != 0 else {
                defer { g_error_free(error)}
                throw ArrowError.arrayError(message: error.map {String(cString: $0.pointee.message) } ?? "")
            }
        })

        guard let garray = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(arrayBuilder), &error) else {
            defer { g_error_free(error)}
            throw ArrowError.arrayError(message: error.map {String(cString: $0.pointee.message) } ?? "")
        }
        ptr = garray
    }
    
    deinit {
        g_object_unref(ptr)
    }
}

public enum ArrowCompressionType {
    case uncompressed
    case snappy
    case gzip
    case brotli
    case zstd
    case lz4
    case lzo
    case bz2
    
    var gtype: GArrowCompressionType {
        switch self {
        case .uncompressed:
            return GARROW_COMPRESSION_TYPE_UNCOMPRESSED
        case .snappy:
            return GARROW_COMPRESSION_TYPE_SNAPPY
        case .gzip:
            return GARROW_COMPRESSION_TYPE_GZIP
        case .brotli:
            return GARROW_COMPRESSION_TYPE_BROTLI
        case .zstd:
            return GARROW_COMPRESSION_TYPE_ZSTD
        case .lz4:
            return GARROW_COMPRESSION_TYPE_LZ4
        case .lzo:
            return GARROW_COMPRESSION_TYPE_LZO
        case .bz2:
            return GARROW_COMPRESSION_TYPE_BZ2
        }
    }
}

public final class ParquetWriterProperties {
    let ptr: UnsafeMutablePointer<GParquetWriterProperties>
    
    public init() {
        guard let properties = gparquet_writer_properties_new() else {
            fatalError("gparquet_writer_properties_new failed")
        }
        ptr = properties
    }
    
    public func setCompression(type: ArrowCompressionType, path: String) {
        gparquet_writer_properties_set_compression(ptr, type.gtype, path)
    }
    
    public func setBatchSize(_ size: Int) {
        gparquet_writer_properties_set_batch_size(ptr, gint64(size))
    }
    
    public func setDataPageSize(_ size: Int) {
        gparquet_writer_properties_set_data_page_size(ptr, gint64(size))
    }
    
    public func setMaxRowGroup(_ length: Int) {
        gparquet_writer_properties_set_max_row_group_length(ptr, gint64(length))
    }
    
    deinit {
        g_object_unref(ptr)
    }
}


public final class ParquetFileWriter {
    var ptr: UnsafeMutablePointer<GParquetArrowFileWriter>?
    
    public init(path: String, schema: ArrowSchema, properties: ParquetWriterProperties = .init()) throws {
        var error: UnsafeMutablePointer<GError>? = nil
        guard let writer = gparquet_arrow_file_writer_new_path(schema.ptr, path, properties.ptr, &error) else {
            defer { g_error_free(error)}
            throw ArrowError.fileWriterError(message: error.map {String(cString: $0.pointee.message) } ?? "")
        }
        ptr = writer
    }
    
    public func write(table: ArrowTable) throws {
        var error: UnsafeMutablePointer<GError>? = nil
        guard gparquet_arrow_file_writer_write_table(ptr, table.ptr, 10, &error) != 0 else {
            defer { g_error_free(error)}
            throw ArrowError.fileWriterError(message: error.map {String(cString: $0.pointee.message) } ?? "")
        }
    }
    
    public func close() throws {
        var error: UnsafeMutablePointer<GError>? = nil
        guard gparquet_arrow_file_writer_close(ptr, &error) != 0 else {
            defer { g_error_free(error)}
            throw ArrowError.fileWriterError(message: error.map {String(cString: $0.pointee.message) } ?? "")
        }
        g_object_unref(ptr)
        ptr = nil
    }
    
    deinit {
        guard ptr == nil else {
            fatalError("ParquetFileWriter.close() was not called")
        }
    }
}

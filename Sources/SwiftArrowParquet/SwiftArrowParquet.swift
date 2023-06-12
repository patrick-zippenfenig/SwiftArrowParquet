import CApacheArrowGlib
import CApacheParquetGlib


enum ArrowError: Error {
    case schemaError
    case tableError(message: String)
    case arrayError(message: String)
    case dataTypeError(message: String)
    case fileWriterError(message: String)
}

/**
 https://arrow.apache.org/docs/c_glib/arrow-glib/arrow-glib-GArrowType.html
 */
public enum ArrowTimeUnit {
    case second
    case millisecond
    case microsecond
    case nanosecond
    
    var gtype: GArrowTimeUnit {
        switch self {
        case .second:
            return GARROW_TIME_UNIT_SECOND
        case .millisecond:
            return GARROW_TIME_UNIT_MILLI
        case .microsecond:
            return GARROW_TIME_UNIT_MICRO
        case .nanosecond:
            return GARROW_TIME_UNIT_NANO
        }
    }
}
/**
 https://arrow.apache.org/docs/c_glib/arrow-glib/basic-data-type-classes.html
 */
public enum ArrowDataType {
    case null
    case boolean
    case int8
    case uint8
    case int16
    case uint16
    case int32
    case uint32
    case int64
    case uint64
    case halfFloat
    case float
    case double
    case binary
    case fixedSizeBinary(byteWith: Int32)
    case largeBinary
    case string
    case largeString
    case date32
    case date64
    case timestamp(unit: ArrowTimeUnit)
    case time32(unit: ArrowTimeUnit)
    case time64(unit: ArrowTimeUnit)
    case monthInterval
    case dayTimeInterval
    case monthDayNanoInterval
    case decimal(precision: Int32, scale: Int32)
    case decimal128(precision: Int32, scale: Int32)
    case decimal256(precision: Int32, scale: Int32)
    
    func toGDatatype() throws -> UnsafeMutablePointer<GArrowDataType>? {
        var error: UnsafeMutablePointer<GError>? = nil
        switch self {
        case .null:
            return GARROW_DATA_TYPE(garrow_null_data_type_new())
        case .boolean:
            return GARROW_DATA_TYPE(garrow_boolean_data_type_new())
        case .int8:
            return GARROW_DATA_TYPE(garrow_int8_data_type_new())
        case .uint8:
            return GARROW_DATA_TYPE(garrow_uint8_data_type_new())
        case .int16:
            return GARROW_DATA_TYPE(garrow_int16_data_type_new())
        case .uint16:
            return GARROW_DATA_TYPE(garrow_uint16_data_type_new())
        case .int32:
            return GARROW_DATA_TYPE(garrow_int32_data_type_new())
        case .uint32:
            return GARROW_DATA_TYPE(garrow_uint32_data_type_new())
        case .int64:
            return GARROW_DATA_TYPE(garrow_int64_data_type_new())
        case .uint64:
            return GARROW_DATA_TYPE(garrow_uint64_data_type_new())
        case .halfFloat:
            return GARROW_DATA_TYPE(garrow_half_float_data_type_new())
        case .float:
            return GARROW_DATA_TYPE(garrow_float_data_type_new())
        case .double:
            return GARROW_DATA_TYPE(garrow_double_data_type_new())
        case .binary:
            return GARROW_DATA_TYPE(garrow_binary_data_type_new())
        case .fixedSizeBinary(byteWith: let byteWith):
            return GARROW_DATA_TYPE(garrow_fixed_size_binary_data_type_new(byteWith))
        case .largeBinary:
            return GARROW_DATA_TYPE(garrow_large_binary_data_type_new())
        case .string:
            return GARROW_DATA_TYPE(garrow_string_data_type_new())
        case .largeString:
            return GARROW_DATA_TYPE(garrow_large_string_data_type_new())
        case .date32:
            return GARROW_DATA_TYPE(garrow_date32_data_type_new())
        case .date64:
            return GARROW_DATA_TYPE(garrow_date64_data_type_new())
        case .timestamp(let unit):
            return GARROW_DATA_TYPE(garrow_timestamp_data_type_new(unit.gtype))
        case .time32(let unit):
            guard let type = garrow_time32_data_type_new(unit.gtype, &error) else {
                defer { g_error_free(error)}
                throw ArrowError.dataTypeError(message: error.map {String(cString: $0.pointee.message) } ?? "")
            }
            return GARROW_DATA_TYPE(type)
        case .time64(let unit):
            guard let type = garrow_time64_data_type_new(unit.gtype, &error) else {
                defer { g_error_free(error)}
                throw ArrowError.dataTypeError(message: error.map {String(cString: $0.pointee.message) } ?? "")
            }
            return GARROW_DATA_TYPE(type)
        case .monthInterval:
            return GARROW_DATA_TYPE(garrow_month_interval_data_type_new())
        case .dayTimeInterval:
            return GARROW_DATA_TYPE(garrow_day_time_interval_data_type_new())
        case .monthDayNanoInterval:
            return GARROW_DATA_TYPE(garrow_month_day_nano_interval_data_type_new())
        case .decimal(let precision, let scale):
            guard let type = garrow_decimal_data_type_new(precision, scale, &error) else {
                defer { g_error_free(error)}
                throw ArrowError.dataTypeError(message: error.map {String(cString: $0.pointee.message) } ?? "")
            }
            return GARROW_DATA_TYPE(type)
        case .decimal128(let precision, let scale):
            guard let type = garrow_decimal128_data_type_new(precision, scale, &error) else {
                defer { g_error_free(error)}
                throw ArrowError.dataTypeError(message: error.map {String(cString: $0.pointee.message) } ?? "")
            }
            return GARROW_DATA_TYPE(type)
        case .decimal256(let precision, let scale):
            guard let type = garrow_decimal256_data_type_new(precision, scale, &error) else {
                defer { g_error_free(error)}
                throw ArrowError.dataTypeError(message: error.map {String(cString: $0.pointee.message) } ?? "")
            }
            return GARROW_DATA_TYPE(type)
        }
    }
}



public final class ArrowSchema {
    let ptr: UnsafeMutablePointer<GArrowSchema>
                                    
    public init(_ columns: [String: ArrowDataType]) throws {
        var fields: UnsafeMutablePointer<GList>?
        for (column, type) in columns {
            //let dataType = garrow_array_get_value_data_type(array.ptr)
            let field = garrow_field_new(column, try type.toGDatatype())
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

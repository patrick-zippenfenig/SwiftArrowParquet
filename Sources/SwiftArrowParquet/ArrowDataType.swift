import CApacheArrowGlib


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

import CApacheArrowGlib


public final class ArrowArray {
    let ptr: UnsafeMutablePointer<GArrowArray>
    
    public init(float array: [Float]) throws {
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
    
    public init(timestamp array: [Int64], unit: ArrowTimeUnit) throws {
        var error: UnsafeMutablePointer<GError>?
        let unitType = garrow_timestamp_data_type_new(unit.gtype)
        defer { g_object_unref(unitType) }
        let arrayBuilder = garrow_timestamp_array_builder_new(unitType)
        defer { g_object_unref(arrayBuilder) }
        try array.withUnsafeBufferPointer( { ptr in
            let p = UnsafeMutableRawPointer(mutating: ptr.baseAddress)?.assumingMemoryBound(to: gint64.self)
            guard garrow_timestamp_array_builder_append_values(arrayBuilder, p, gint64(ptr.count), [], 0, &error) != 0 else {
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
    
    public init(int64 array: [Int64]) throws {
        var error: UnsafeMutablePointer<GError>?
        let arrayBuilder = garrow_int64_array_builder_new()
        defer { g_object_unref(arrayBuilder) }
        try array.withUnsafeBufferPointer( { ptr in
            let p = UnsafeMutableRawPointer(mutating: ptr.baseAddress)?.assumingMemoryBound(to: gint64.self)
            guard garrow_int64_array_builder_append_values(arrayBuilder, p, gint64(ptr.count), [], 0, &error) != 0 else {
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

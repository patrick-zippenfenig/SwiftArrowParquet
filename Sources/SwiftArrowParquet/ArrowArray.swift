import CApacheArrowGlib


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

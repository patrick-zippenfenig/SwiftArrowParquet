import CApacheArrowGlib


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

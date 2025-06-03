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
    
    /// The number of rows in the table.
    public var rowCount: UInt64 {
        return UInt64(garrow_table_get_n_rows(ptr))
    }
    
    /// The number of columns in the table.
    public var columnCount: UInt {
        return UInt(garrow_table_get_n_columns(ptr))
    }
    
    /// The formatted table content. Throws on error
    public func toString() throws -> String {
        var error: UnsafeMutablePointer<GError>? = nil
        guard let cString = garrow_table_to_string(ptr, &error) else {
            defer { g_error_free(error)}
            throw ArrowError.tableError(message: error.map {String(cString: $0.pointee.message) } ?? "")
        }
        defer { g_free(cString)}
        return String(cString: cString)
    }
    
    deinit {
        g_object_unref(ptr)
    }
}

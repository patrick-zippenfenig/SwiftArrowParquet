import CApacheArrowGlib


public final class ArrowSchema {
    let ptr: UnsafeMutablePointer<GArrowSchema>
                                    
    public init(_ columns: [(name: String, type: ArrowDataType)]) throws {
        var fields: UnsafeMutablePointer<GList>?
        for (column, type) in columns {
            //let dataType = garrow_array_get_value_data_type(array.ptr)
            let type = try type.toGDatatype()
            defer { g_object_unref(type) }
            let field = garrow_field_new(column, type)
            fields = g_list_prepend(fields, field)
        }
        fields = g_list_reverse(fields)
        guard let schema = garrow_schema_new(fields) else {
            throw ArrowError.schemaError
        }
        ptr = schema
    }
    
    /// Number of fields in the schema
    public var countFields: UInt32 {
        garrow_schema_n_fields(ptr)
    }
    
    /// The string representation of the schema.
    public func toString() -> String {
        guard let cString = garrow_schema_to_string(ptr) else {
            return ""
        }
        defer { g_free(cString)}
        return String(cString: cString)
    }
    
    deinit {
        g_object_unref(ptr)
    }
}

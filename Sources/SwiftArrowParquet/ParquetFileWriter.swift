import CApacheParquetGlib


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
    
    public func write(table: ArrowTable, chunkSize: Int) throws {
        var error: UnsafeMutablePointer<GError>? = nil
        guard gparquet_arrow_file_writer_write_table(ptr, table.ptr, gsize(chunkSize), &error) != 0 else {
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

import CApacheParquetGlib


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

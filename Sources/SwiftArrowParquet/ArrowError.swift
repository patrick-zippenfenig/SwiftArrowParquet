

enum ArrowError: Error {
    case schemaError
    case tableError(message: String)
    case arrayError(message: String)
    case dataTypeError(message: String)
    case fileWriterError(message: String)
}

import enum


class CompressionType(enum.StrEnum):
    NONE = "NONE"
    GZIP = "GZIP"
    BZIP2 = "BZIP2"


class FileType(enum.StrEnum):
    CSV = "CSV"
    JSON = "JSON"
    SQLITE = "SQLITE"
    PARQUET = "PARQUET"

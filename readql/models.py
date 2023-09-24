from enum import Enum


class CompressionType(str, Enum):
    NONE = 'NONE'
    GZIP = 'GZIP'
    BZIP2 = 'BZIP2'


class FileType(str, Enum):
    CSV = 'CSV'
    JSON = 'JSON'
    SQLITE = 'SQLITE'
    PARQUET = 'PARQUET'

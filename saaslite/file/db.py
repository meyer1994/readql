import logging
import warnings

import apsw

from saaslite.file.base import FileBase

logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore', category=DeprecationWarning, module=__name__)


def DictRowFactory(cursor, row: tuple) -> dict:
    description = cursor.getdescription()
    return {k[0]: row[i] for i, k in enumerate(description)}


class S3VFS(apsw.VFS):
    def __init__(self, client, bucket_name):
        self.name = 's3'
        self.client = client
        self.bucket_name = bucket_name
        apsw.VFS.__init__(self, self.name, base='')

    def xOpen(self, name, flags):
        logger.info('Opening file: s3://%s/%s', self.bucket_name, name.filename())  # noqa
        return S3VFSFile(name, flags, self.client, self.bucket_name)


class S3VFSFile(object):
    def __init__(self, name, flags, client, bucket):
        self.name = name.filename().lstrip('/')
        self.flags = flags
        self.client = client
        self.bucket = bucket

    def xRead(self, amount, offset):
        ranged = f'bytes={offset}-{offset + amount}'
        logger.info('Fetching range for %s: %s', self.name, ranged)
        response = self.client\
            .get_object(Bucket=self.bucket, Key=self.name, Range=ranged)
        return response['Body'].read()

    def xFileSize(self):
        response = self.client.head_object(Bucket=self.bucket, Key=self.name)
        size = response['ContentLength']
        logger.info('File size for file %s: %d', self.name, size)
        return size

    def xClose(self):
        pass

    def xUnlock(self, level):
        pass

    def xLock(self, level):
        pass

    def xFileControl(self, op, ptr):
        return False


class FileDB(FileBase):
    FLAGS = apsw.SQLITE_OPEN_READONLY | apsw.SQLITE_OPEN_URI

    def sql(self, sql: str) -> list:
        logger.info('Querying s3://%s/%s: %s', self.bucket_name, self.bucket_key, sql)  # noqa

        file = f'file:/{self.bucket_key}'
        vfs = S3VFS(self.client, self.bucket_name)

        connection = apsw.Connection(file, flags=self.FLAGS, vfs=vfs.name)
        connection.setrowtrace(DictRowFactory)

        cursor = connection.cursor()

        # Most were copied from:
        #   https://avi.im/blag/2021/fast-sqlite-inserts/
        cursor.execute('PRAGMA page_size = 65536')
        cursor.execute('PRAGMA journal_mode = OFF')
        cursor.execute('PRAGMA synchronous = 0')
        cursor.execute('PRAGMA cache_size = 1000000')
        cursor.execute('PRAGMA locking_mode = EXCLUSIVE')
        cursor.execute('PRAGMA temp_store = MEMORY')

        yield from cursor.execute(sql)

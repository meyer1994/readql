import logging
import warnings
from typing import Iterable

import apsw
from mypy_boto3_s3 import Client

from readql.tables.base import Base

logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore", category=DeprecationWarning, module=__name__)


def DictRowFactory(cursor, row: tuple) -> dict:
    description = cursor.getdescription()
    return {k[0]: row[i] for i, k in enumerate(description)}


class S3VFS(apsw.VFS):
    def __init__(self, client: Client, bucket: str):
        self.name = "s3"
        self.client = client
        self.bucket = bucket
        apsw.VFS.__init__(self, self.name, base="")

    def xOpen(self, name: str, flags: int):
        filename = name.filename()
        logger.debug("Opening file: s3://%s/%s", self.bucket, filename)
        return S3VFSFile(name, flags, self.client, self.bucket)


class S3VFSFile(object):
    def __init__(self, name, flags: int, client: Client, bucket: str):
        self.name = name.filename().lstrip("/")
        self.flags = flags
        self.client = client
        self.bucket = bucket

    def xRead(self, amount, offset) -> bytes:
        ranged = f"bytes={offset}-{offset + amount}"
        logger.debug("Fetching range for %s: %s", self.name, ranged)
        response = self.client.get_object(
            Bucket=self.bucket, Key=self.name, Range=ranged
        )
        return response["Body"].read()

    def xFileSize(self) -> int:
        logger.info("Fetching file size s3:/%s/%s", self.bucket, self.name)
        response = self.client.head_object(Bucket=self.bucket, Key=self.name)
        size = response["ContentLength"]
        logger.info("File s3:/%s/%s has size %d", self.bucket, self.name, size)
        return size

    def xClose(self):
        pass

    def xUnlock(self, level):
        pass

    def xLock(self, level):
        pass

    def xFileControl(self, op, ptr) -> bool:
        return False


class Sqlite(Base):
    FLAGS = apsw.SQLITE_OPEN_READONLY | apsw.SQLITE_OPEN_URI

    def sql(self, sql: str) -> Iterable[dict]:
        logger.info("Querying %s: %s", self.uri, sql)

        file = f"file:/{self.key}"
        vfs = S3VFS(self.client, self.bucket)

        connection = apsw.Connection(file, flags=self.FLAGS, vfs=vfs.name)
        connection.setrowtrace(DictRowFactory)

        cursor = connection.cursor()
        yield from cursor.execute(sql)

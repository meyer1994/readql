import warnings

import apsw
import boto3


warnings.filterwarnings('ignore', category=DeprecationWarning, module=__name__)


def DictRowFactory(cursor, row):
    description = cursor.getdescription()
    return {k[0]: row[i] for i, k in enumerate(description)}


class S3VFS(apsw.VFS):
    def __init__(self, client, bucket):
        self.name = 's3'
        self.client = client
        self.bucket = bucket
        apsw.VFS.__init__(self, self.name, base='')

    def xOpen(self, name, flags):
        return S3VFSFile(name, flags, self.client, self.bucket)


class S3VFSFile(object):
    def __init__(self, name, flags, client, bucket):
        self.name = name.filename().lstrip('/')
        self.flags = flags
        self.client = client
        self.bucket = bucket

    def xRead(self, amount, offset):
        ranged = f'bytes={offset}-{offset + amount}'
        response = self.client\
            .get_object(Bucket=self.bucket, Key=self.name, Range=ranged)
        return response['Body'].read()

    def xFileSize(self):
        response = self.client.head_object(Bucket=self.bucket, Key=self.name)
        return response['ContentLength']

    def xClose(self):
        pass

    def xUnlock(self, level):
        pass

    def xLock(self, level):
        pass

    def xFileControl(self, op, ptr):
        return False

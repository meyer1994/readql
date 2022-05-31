import logging
from abc import ABC, abstractmethod

import boto3
from botocore.errorfactory import ClientError

logger = logging.getLogger(__name__)


class FileBase(ABC):
    def __init__(self, bucket_region: str, bucket_name: str, bucket_key: str):
        super(FileBase, self).__init__()
        self.bucket_region = bucket_region
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key

    @property
    def client(self):
        return boto3.client('s3', region_name=self.bucket_region)

    @abstractmethod
    def sql(self, sql: str) -> list:
        pass

    def exists(self) -> bool:
        """
        Adapted from:
            https://stackoverflow.com/a/38376288
        """
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=self.bucket_key)  # noqa
        except ClientError as e:
            logger.exception(e)
            return False
        return True

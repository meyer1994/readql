import json
import logging
from typing import Iterable
from dataclasses import dataclass, field
from abc import ABC, abstractmethod

from mypy_boto3_s3 import Client
from botocore.errorfactory import ClientError


logger = logging.getLogger(__name__)


@dataclass
class Base(ABC):
    client: Client
    bucket: str = field(repr=False)
    key: str = field(repr=False)
    uri: str = field(init=False)

    def __post_init__(self):
        self.uri = f's3://{self.bucket}/{self.key}'

    @abstractmethod
    def sql(self, sql: str) -> Iterable[dict]:
        pass

    def _sql(self, sql: str, conf: dict) -> Iterable[dict]:
        logger.info('Querying %s: %s', self.uri, sql)
        response = self.client.select_object_content(
            Bucket=self.bucket,
            Key=self.key,
            ExpressionType='SQL',
            Expression=sql,
            InputSerialization=conf,
            OutputSerialization={'JSON': {}},
        )

        if 'Payload' not in response:
            logger.info('No payload in response %s', response)
            return

        for payload in response['Payload']:
            if 'Records' not in payload:
                logger.info('No records in payload: %s', payload)
                continue

            records = payload['Records']['Payload']
            records = records.split(b'\n')
            records.pop()  # last one, after splitting, is always empty

            for record in records:
                yield json.loads(record)

    def exists(self) -> bool:
        """
        Adapted from:
            https://stackoverflow.com/a/38376288
        """
        logger.info('Checking existence of %s', self.uri)

        try:
            self.client.head_object(Bucket=self.bucket, Key=self.key)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.info('Object %s does not exist', self.uri)
                return False
            raise e

        logger.info('Object %s exists', self.uri)
        return True

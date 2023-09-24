from dataclasses import dataclass, field

import boto3
from mypy_boto3_s3 import Client


@dataclass
class UrlGen:
    session: boto3.Session
    bucket: str
    client: Client = field(init=False)

    def __post_init__(self):
        self.client = self.session.client('s3')

    def generate(self, key: str, seconds: int) -> str:
        params = {'Bucket': self.bucket, 'Key': key}
        return self.client.generate_presigned_url(
            Params=params,
            ExpiresIn=seconds,
            ClientMethod='put_object'
        )

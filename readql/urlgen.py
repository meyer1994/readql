from dataclasses import dataclass

from mypy_boto3_s3 import Client


@dataclass
class UrlGen:
    client: Client
    bucket: str

    def generate(self, key: str, seconds: int) -> str:
        params = {'Bucket': self.bucket, 'Key': key}
        return self.client.generate_presigned_url(
            Params=params,
            ExpiresIn=seconds,
            ClientMethod='put_object'
        )

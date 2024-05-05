from dataclasses import dataclass

from mypy_boto3_s3 import Client


@dataclass
class UrlGen:
    client: Client
    bucket: str

    def generate(self, key: str, seconds: int) -> str:
        return self.client.generate_presigned_url(
            ExpiresIn=seconds,
            ClientMethod="put_object",
            Params={"Bucket": self.bucket, "Key": key},
        )

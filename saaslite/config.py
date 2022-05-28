import boto3
from pydantic import BaseSettings


class _Settings(BaseSettings):
    AWS_REGION: str
    SAASLITE_S3_BUCKET_NAME: str

    @property
    def s3(self):
        return boto3.client('s3', region_name=self.AWS_REGION)


async def Settings() -> _Settings:
    return _Settings()

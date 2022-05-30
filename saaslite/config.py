import boto3
from pydantic import BaseSettings


class _Settings(BaseSettings):
    SAASLITE_S3_BUCKET_NAME: str
    SAASLITE_S3_BUCKET_REGION: str

    @property
    def s3(self):
        return boto3.client('s3', region_name=self.SAASLITE_S3_BUCKET_REGION)


async def Settings() -> _Settings:
    return _Settings()

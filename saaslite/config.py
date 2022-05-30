from pydantic import BaseSettings


class _Settings(BaseSettings):
    SAASLITE_S3_BUCKET_NAME: str
    SAASLITE_S3_BUCKET_REGION: str


async def Settings() -> _Settings:
    return _Settings()

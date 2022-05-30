from pydantic import BaseSettings


class Config(BaseSettings):
    SAASLITE_S3_BUCKET_NAME: str
    SAASLITE_S3_BUCKET_REGION: str


async def Conf() -> Config:
    return Config()

from pydantic_settings import BaseSettings


class Config(BaseSettings):
    READQL_S3_BUCKET_NAME: str

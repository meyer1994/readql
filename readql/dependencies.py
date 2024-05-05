from typing import Annotated

import boto3
from fastapi import Depends
from mypy_boto3_s3 import Client

from readql import config, urlgen


def get_config() -> config.Config:
    return config.Config()


def get_s3_client() -> Client:
    return boto3.client("s3")


GetConfig = Annotated[config.Config, Depends(get_config)]
GetS3Client = Annotated[Client, Depends(get_s3_client)]


def get_urlgen(client: GetS3Client, config: GetConfig) -> urlgen.UrlGen:
    return urlgen.UrlGen(client, config.READQL_S3_BUCKET_NAME)


GetUrlGen = Annotated[urlgen.UrlGen, Depends(get_urlgen)]

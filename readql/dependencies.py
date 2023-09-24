from typing import Annotated

import boto3
from fastapi import Depends
from mypy_boto3_s3 import Client

from readql import config
from readql import urlgen



def _Config() -> config.Config:
    return config.Config()


def _Client() -> Client:
    return boto3.client('s3')


Config = Annotated[config.Config, Depends(_Config)]
Client = Annotated[Client, Depends(_Client)]


def _UrlGen(client: Client, config: Config) -> urlgen.UrlGen:
    return urlgen.UrlGen(client, config.READQL_S3_BUCKET_NAME)


UrlGen = Annotated[urlgen.UrlGen, Depends(_UrlGen)]

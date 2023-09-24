from typing import Annotated

import boto3
from fastapi import Depends

from readql import config
from readql import urlgen



def _Config() -> config.Config:
    return config.Config()


def _Session() -> boto3.Session:
    return boto3.Session()


Config = Annotated[config.Config, Depends(_Config)]
Session = Annotated[boto3.Session, Depends(_Session)]


def _UrlGen(session: Session, config: Config) -> urlgen.UrlGen:
    return urlgen.UrlGen(session, config.READQL_S3_BUCKET_NAME)


UrlGen = Annotated[urlgen.UrlGen, Depends(_UrlGen)]

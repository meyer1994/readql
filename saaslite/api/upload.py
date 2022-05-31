import uuid
import logging
from enum import Enum
from dataclasses import dataclass

from fastapi import APIRouter, Depends, HTTPException

from saaslite.api import info
from saaslite.url import Presigned
from saaslite.config import Conf

logger = logging.getLogger(__name__)
router = APIRouter()


class File(str, Enum):
    CSV = 'CSV'
    DB = 'DB'


@dataclass
class Post:
    file: File = File.DB
    conf: Conf = Depends(Conf)


@router.post('/', **info.UPLOAD_POST)
async def post(ctx: Post = Depends(Post)) -> dict:
    logger.info('%s', ctx)

    bucket_name = ctx.conf.SAASLITE_S3_BUCKET_NAME
    bucket_region = ctx.conf.SAASLITE_S3_BUCKET_REGION

    key = uuid.uuid4()

    if ctx.kind == File.DB:
        key = f'{key}.db'
    if ctx.kind == File.CSV:
        key = f'{key}.csv'

    url = Presigned(bucket_region, bucket_name)

    return {
        'object_key': key,
        'upload_url': url.generate(key)
    }

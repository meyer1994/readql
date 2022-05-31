import uuid
import logging
from enum import Enum
from dataclasses import dataclass

from fastapi import APIRouter, Depends, HTTPException

from saaslite.url import Presigned
from saaslite.config import Conf

logger = logging.getLogger(__name__)
router = APIRouter()


class Kind(str, Enum):
    CSV = 'csv'
    SQLITE = 'sqlite'


@dataclass
class Post:
    kind: Kind = Kind.SQLITE
    conf: Conf = Depends(Conf)


@router.post('/')
async def post(ctx: Post = Depends(Post)) -> dict:
    logger.info('%s', ctx)

    bucket_name = ctx.conf.SAASLITE_S3_BUCKET_NAME
    bucket_region = ctx.conf.SAASLITE_S3_BUCKET_REGION

    key = uuid.uuid4()

    if ctx.kind == Kind.SQLITE:
        key = f'{key}.db'
    if ctx.kind == Kind.CSV:
        key = f'{key}.csv'

    url = Presigned(bucket_region, bucket_name)

    return {
        'object_key': key,
        'upload_url': url.generate(key)
    }

import logging
from enum import Enum
from dataclasses import dataclass

from fastapi import APIRouter, Depends, HTTPException

from saaslite.config import Conf
from saaslite.file import FileDB

logger = logging.getLogger(__name__)
router = APIRouter()


@dataclass
class Query:
    q: str
    filename: str
    conf: Conf = Depends(Conf)


@router.get('/{filename}.db')
async def get(ctx: Query = Depends(Query)) -> list:
    logger.info('%s', ctx)

    bucket_key = f'{ctx.filename}.db'
    bucket_name = ctx.conf.SAASLITE_S3_BUCKET_NAME
    bucket_region = ctx.conf.SAASLITE_S3_BUCKET_REGION

    select = FileDB(bucket_region, bucket_name, bucket_key)

    if not select.exists():
        detail = f'Database {bucket_key} not found'
        raise HTTPException(status_code=404, detail=detail)

    return select.sql(ctx.q)

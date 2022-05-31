import logging
from enum import Enum
from dataclasses import dataclass

from fastapi import APIRouter, Depends, HTTPException
from pydantic import constr

from saaslite.config import Conf
from saaslite.file import FileCSV

logger = logging.getLogger(__name__)
router = APIRouter()


class Header(str, Enum):
    USE = 'USE'
    NONE = 'NONE'
    IGNORE = 'IGNORE'


@dataclass
class Query:
    q: str
    filename: str
    header: Header = Header.NONE
    delimiter: constr(min_length=1, max_length=1) = ','
    conf: Conf = Depends(Conf)


@router.get('/{filename}.csv')
async def get(ctx: Query = Depends(Query)) -> list:
    logger.info('%s', ctx)

    bucket_key = f'{ctx.filename}.csv'
    bucket_name = ctx.conf.SAASLITE_S3_BUCKET_NAME
    bucket_region = ctx.conf.SAASLITE_S3_BUCKET_REGION

    select = FileCSV(bucket_region, bucket_name, bucket_key)

    if not select.exists():
        detail = f'Database {bucket_key} not found'
        raise HTTPException(status_code=404, detail=detail)

    return select.sql(ctx.q, ctx.delimiter, ctx.header.value)

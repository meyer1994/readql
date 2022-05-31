import uuid
import logging
from enum import Enum
from dataclasses import dataclass

from fastapi import FastAPI, Depends, HTTPException
from pydantic import constr

from saaslite.url import Presigned
from saaslite.select import Select
from saaslite.config import Conf

logger = logging.getLogger(__name__)
app = FastAPI()


@dataclass
class QueryDB:
    q: str
    filename: str
    conf: Conf = Depends(Conf)


@app.get('/{filename}.db')
async def query_db(ctx: QueryDB = Depends(QueryDB)) -> list:
    logger.info('%s', ctx)
    bucket_key = f'{ctx.filename}.db'
    bucket_name = ctx.conf.SAASLITE_S3_BUCKET_NAME
    bucket_region = ctx.conf.SAASLITE_S3_BUCKET_REGION

    select = Select(bucket_region, bucket_name, bucket_key)

    if select.exists():
        return select.sql(ctx.q)

    detail = f'Database {bucket_key} not found'
    raise HTTPException(status_code=404, detail=detail)


class Header(str, Enum):
    USE = 'USE'
    NONE = 'NONE'
    IGNORE = 'IGNORE'


@dataclass
class QueryCSV:
    q: str
    filename: str
    header: Header = Header.NONE
    delimiter: constr(min_length=1, max_length=1) = ','
    conf: Conf = Depends(Conf)


@app.get('/{filename}.csv')
async def query_csv(ctx: QueryCSV = Depends(QueryCSV)) -> list:
    logger.info('%s', ctx)
    bucket_key = f'{ctx.filename}.csv'
    bucket_name = ctx.conf.SAASLITE_S3_BUCKET_NAME
    bucket_region = ctx.conf.SAASLITE_S3_BUCKET_REGION

    select = Select(bucket_region, bucket_name, bucket_key)

    if select.exists():
        return select.sql(ctx.q, ctx.delimiter, ctx.header.value)

    detail = f'Database {bucket_key} not found'
    raise HTTPException(status_code=404, detail=detail)


class Kind(str, Enum):
    CSV = 'csv'
    SQLITE = 'sqlite'


@dataclass
class Post:
    kind: Kind = Kind.SQLITE
    conf: Conf = Depends(Conf)


@app.post('/')
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
        'upload_url': url.upload(key)
    }

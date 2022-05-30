import uuid
from enum import Enum
from dataclasses import dataclass

from fastapi import FastAPI, Depends, HTTPException

from saaslite.url import Presigned
from saaslite.select import Select
from saaslite.config import Conf


app = FastAPI()


@dataclass
class Query:
    q: str
    filename: str
    conf: Conf = Depends(Conf)


@app.get('/{filename}')
async def query(ctx: Query = Depends(Query)) -> list:
    bucket_key = ctx.filename
    bucket_name = ctx.conf.SAASLITE_S3_BUCKET_NAME
    bucket_region = ctx.conf.SAASLITE_S3_BUCKET_REGION

    select = Select(bucket_region, bucket_name, bucket_key)

    if select.exists():
        return select.sql(ctx.q)

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

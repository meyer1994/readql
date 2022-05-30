import uuid
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


@dataclass
class Post:
    kind: str = 'sqlite'
    conf: Conf = Depends(Conf)


@app.post('/')
async def post(ctx: Post = Depends(Post)) -> dict:
    bucket_name = ctx.conf.SAASLITE_S3_BUCKET_NAME
    bucket_region = ctx.conf.SAASLITE_S3_BUCKET_REGION

    key = '%s.db' % uuid.uuid4()
    url = Presigned(bucket_region, bucket_name)

    return {
        'object_key': key,
        'upload_url': url.upload(key)
    }

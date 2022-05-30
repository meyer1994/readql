from dataclasses import dataclass

from fastapi import FastAPI, Depends, HTTPException

from saaslite import select
from saaslite.config import Settings


app = FastAPI()


@dataclass
class QueryDeps:
    q: str
    filename: str
    settings: Settings = Depends(Settings)


@app.get('/{filename}')
async def query_db(ctx: QueryDeps = Depends(QueryDeps)) -> list:
    bucket_key = ctx.filename
    bucket_name = ctx.settings.SAASLITE_S3_BUCKET_NAME
    bucket_region = ctx.settings.SAASLITE_S3_BUCKET_REGION

    file = select.Select(bucket_region, bucket_name, bucket_key)

    if file.exists():
        return file.sql(ctx.q)

    detail = f'Database {bucket_key} not found'
    raise HTTPException(status_code=404, detail=detail)

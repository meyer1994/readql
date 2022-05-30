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


@app.get('/{filename}.db')
async def query_sql(ctx: QueryDeps = Depends(QueryDeps)) -> list:
    bucket_key = f'{ctx.filename}.db'
    bucket_name = ctx.settings.SAASLITE_S3_BUCKET_NAME
    bucket_region = ctx.settings.SAASLITE_S3_BUCKET_REGION

    sqlite = select.SelectSQLite(bucket_region, bucket_name, bucket_key)

    if sqlite.exists():
        return sqlite.sql(ctx.q)

    detail = f'Database {bucket_key} not found'
    raise HTTPException(status_code=404, detail=detail)

from dataclasses import dataclass

import apsw
import boto3
from fastapi import FastAPI, Depends

from saaslite import sql
from saaslite.config import Settings


app = FastAPI()


@dataclass
class QueryDeps:
    q: str
    filename: str
    settings: Settings = Depends(Settings)


@app.get('/{filename}')
async def query(ctx: QueryDeps = Depends(QueryDeps)) -> list:
    flags = apsw.SQLITE_OPEN_READONLY | apsw.SQLITE_OPEN_URI
    vfs = sql.S3VFS(ctx.settings.s3, ctx.settings.SAASLITE_S3_BUCKET_NAME)

    file = f'file:/{ctx.filename}'
    connection = apsw.Connection(file, flags=flags, vfs=vfs.name)
    connection.setrowtrace(sql.DictRowFactory)

    cursor = connection.cursor()
    data = cursor.execute(ctx.q)

    return list(data)


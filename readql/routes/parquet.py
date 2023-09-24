from typing import Annotated, Iterable
from dataclasses import dataclass

from fastapi import APIRouter, Query, Path, Depends

import readql.dependencies as deps
from readql.tables import Parquet
from readql.errors import FileNotFoundError


router = APIRouter()


@dataclass
class Context:
    config: deps.Config
    client: deps.Client
    key: str = Path(..., example='test')
    q: str = Query(..., example='SELECT * FROM s3Object')


@router.get('/{key}.parquet')
def parquet(ctx: Annotated[Context, Depends(Context)]) -> Iterable[dict]:
    table = Parquet(
        client=ctx.client, 
        bucket=ctx.config.READQL_S3_BUCKET_NAME, 
        key=f'{ctx.key}.parquet',
    )

    if not table.exists():
        raise FileNotFoundError(f'{ctx.key}.parquet')

    return table.sql(sql=ctx.q)

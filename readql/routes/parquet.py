from enum import Enum
from typing import Annotated, Iterable
from dataclasses import dataclass

from fastapi import APIRouter, Query, Path, Depends

import readql.dependencies as deps
from readql.tables import Parquet
from readql.models import CompressionType
from readql.errors import FileNotFoundError


router = APIRouter()


@dataclass
class Context:
    config: deps.Config
    session: deps.Session
    key: str = Path(..., example='test')
    q: str = Query(..., example='SELECT * FROM s3Object')
    compression: CompressionType = Query(CompressionType.NONE)


@router.get('/{key}.parquet')
def parquet(ctx: Annotated[Context, Depends(Context)]) -> Iterable[dict]:
    table = Parquet(
        session=ctx.session, 
        bucket=ctx.config.READQL_S3_BUCKET_NAME, 
        key=f'{ctx.key}.parquet',
    )

    if not table.exists():
        raise FileNotFoundError(f'{ctx.key}.parquet')

    return table.sql(sql=ctx.q, compression=ctx.compression.value)

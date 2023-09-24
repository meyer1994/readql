from enum import Enum
from typing import Annotated, Iterable
from dataclasses import dataclass

from fastapi import APIRouter, Query, Path, Depends

import readql.dependencies as deps
from readql.tables import JSON
from readql.models import CompressionType
from readql.errors import FileNotFoundError


router = APIRouter()


class JsonType(str, Enum):
    DOCUMENT = 'DOCUMENT'
    LINES = 'LINES'


@dataclass
class Context:
    config: deps.Config
    session: deps.Session
    key: str = Path(..., example='test')
    q: str = Query(..., example='SELECT * FROM s3Object')
    type: JsonType = Query(JsonType.DOCUMENT)
    compression: CompressionType = Query(CompressionType.NONE)


@router.get('/{key}.json')
def json(ctx: Annotated[Context, Depends(Context)]) -> Iterable[dict]:
    table = JSON(
        session=ctx.session, 
        bucket=ctx.config.READQL_S3_BUCKET_NAME, 
        key=f'{ctx.key}.json'
    )

    if not table.exists():
        raise FileNotFoundError(f'{ctx.key}.json')

    return table.sql(
        sql=ctx.q, 
        type=ctx.type.value, 
        compression=ctx.compression.value
    )

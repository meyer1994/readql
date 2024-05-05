from dataclasses import dataclass
from enum import Enum
from typing import Annotated, Iterable

from fastapi import APIRouter, Depends, Path, Query

import readql.dependencies as deps
from readql.errors import FileNotFoundError
from readql.models import CompressionType
from readql.tables import JSON

router = APIRouter()


class JsonType(str, Enum):
    DOCUMENT = "DOCUMENT"
    LINES = "LINES"


@dataclass
class Context:
    config: deps.Config
    client: deps.Client
    key: Annotated[str, Path(example="test")]
    q: Annotated[str, Query(example="SELECT * FROM s3Object")]
    type: Annotated[JsonType, Query()] = JsonType.DOCUMENT
    compression: Annotated[CompressionType, Query()] = CompressionType.NONE


@router.get("/{key}.json")
def json(ctx: Annotated[Context, Depends(Context)]) -> Iterable[dict]:
    table = JSON(
        client=ctx.client,
        bucket=ctx.config.READQL_S3_BUCKET_NAME,
        key=f"{ctx.key}.json",
    )

    if not table.exists():
        raise FileNotFoundError(f"{ctx.key}.json")

    return table.sql(
        sql=ctx.q,
        type=ctx.type.value,
        compression=ctx.compression.value,
    )

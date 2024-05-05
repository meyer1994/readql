from dataclasses import dataclass
from typing import Annotated, Iterable

from fastapi import APIRouter, Depends, Path, Query

import readql.dependencies as deps
from readql.errors import FileNotFoundError
from readql.tables import Sqlite

router = APIRouter()


@dataclass
class Context:
    config: deps.GetConfig
    client: deps.GetS3Client
    key: Annotated[str, Path(..., example="test")]
    q: Annotated[str, Query(..., example="SELECT * FROM test")]


@router.get("/{key}.sqlite")
def sqlite(ctx: Annotated[Context, Depends(Context)]) -> Iterable[dict]:
    table = Sqlite(
        client=ctx.client,
        bucket=ctx.config.READQL_S3_BUCKET_NAME,
        key=f"{ctx.key}.sqlite",
    )

    if not table.exists():
        raise FileNotFoundError(f"{ctx.key}.sqlite")

    return table.sql(sql=ctx.q)

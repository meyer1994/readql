from dataclasses import dataclass
from typing import Annotated, Iterable

from fastapi import APIRouter, Depends, Path, Query

import readql.dependencies as deps
from readql.errors import FileNotFoundError
from readql.tables import Parquet

router = APIRouter()


@dataclass
class Context:
    config: deps.GetConfig
    client: deps.GetS3Client
    key: Annotated[str, Path(example="test")]
    q: Annotated[str, Query(example="SELECT * FROM s3Object")]


@router.get("/{key}.parquet")
def parquet(ctx: Annotated[Context, Depends(Context)]) -> Iterable[dict]:
    table = Parquet(
        client=ctx.client,
        bucket=ctx.config.READQL_S3_BUCKET_NAME,
        key=f"{ctx.key}.parquet",
    )

    if not table.exists():
        raise FileNotFoundError(f"{ctx.key}.parquet")

    return table.sql(sql=ctx.q)

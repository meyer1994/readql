import enum
from dataclasses import dataclass
from typing import Annotated, Iterable

from fastapi import APIRouter, Depends, Path, Query

import readql.dependencies as deps
from readql.errors import FileNotFoundError
from readql.models import CompressionType
from readql.tables import CSV

router = APIRouter()


class CsvHeader(enum.StrEnum):
    USE = "USE"
    NONE = "NONE"
    IGNORE = "IGNORE"


@dataclass
class Context:
    config: deps.GetConfig
    client: deps.GetS3Client
    key: Annotated[str, Path(example="test")]
    q: Annotated[str, Query(example="SELECT * FROM s3Object")]
    header: Annotated[CsvHeader, Query()] = CsvHeader.NONE
    delimiter: Annotated[str, Query(max_length=1, min_length=1)] = ","
    compression: Annotated[CompressionType, Query()] = CompressionType.NONE


@router.get("/{key}.csv")
def csv(ctx: Annotated[Context, Depends(Context)]) -> Iterable[dict]:
    table = CSV(
        client=ctx.client,
        bucket=ctx.config.READQL_S3_BUCKET_NAME,
        key=f"{ctx.key}.csv",
    )

    if not table.exists():
        raise FileNotFoundError(f"{ctx.key}.csv")

    return table.sql(
        sql=ctx.q,
        delimiter=ctx.delimiter,
        header=ctx.header.value,
        compression=ctx.compression.value,
    )

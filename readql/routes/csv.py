from enum import Enum
from typing import Annotated, Iterable
from dataclasses import dataclass

from pydantic import constr
from fastapi import APIRouter, Query, Path, Depends

import readql.dependencies as deps
from readql.tables import CSV
from readql.models import CompressionType
from readql.errors import FileNotFoundError


router = APIRouter()


class CsvHeader(str, Enum):
    USE = 'USE'
    NONE = 'NONE'
    IGNORE = 'IGNORE'


@dataclass
class Context:
    config: deps.Config
    session: deps.Session
    key: str = Path(..., example='test')
    q: str = Query(..., example='SELECT * FROM s3Object')
    header: CsvHeader = Query(CsvHeader.NONE)
    delimiter: constr(min_length=1, max_length=1) = Query(',')
    compression: CompressionType = Query(CompressionType.NONE)


@router.get('/{key}.csv')
def csv(ctx: Annotated[Context, Depends(Context)]) -> Iterable[dict]:
    table = CSV(
        session=ctx.session, 
        bucket=ctx.config.READQL_S3_BUCKET_NAME, 
        key=f'{ctx.key}.csv'
    )

    if not table.exists():
        raise FileNotFoundError(f'{ctx.key}.csv')
    
    return table.sql(
        sql=ctx.q, 
        delimiter=ctx.delimiter, 
        header=ctx.header.value, 
        compression=ctx.compression.value
    )


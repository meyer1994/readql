import uuid
from typing import Annotated
from dataclasses import dataclass, field

from fastapi import APIRouter, Query, Depends
from pydantic import BaseModel

import readql.dependencies as deps
from readql.models import FileType


router = APIRouter()


class UrlGenResponse(BaseModel):
    key: str
    url: str


@dataclass
class Context:
    urlgen: deps.UrlGen
    type: FileType = Query(...)
    uid: uuid.UUID = field(init=False, default_factory=uuid.uuid4)


@router.get('/')
def urlgen(ctx: Annotated[Context, Depends(Context)]) -> UrlGenResponse:
    key = f'{ctx.uid}.{ctx.type.value}'
    key = key.lower()

    url = ctx.urlgen.generate(key, seconds=10)

    return UrlGenResponse(key=key, url=url)

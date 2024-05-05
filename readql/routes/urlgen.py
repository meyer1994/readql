import uuid
from dataclasses import dataclass, field
from typing import Annotated

from fastapi import APIRouter, Depends, Query
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
    type: Annotated[FileType, Query(...)]
    uid: Annotated[uuid.UUID, Query(...)] = field(
        init=False,
        default_factory=uuid.uuid4,
    )


@router.get("/")
def urlgen(ctx: Annotated[Context, Depends(Context)]) -> UrlGenResponse:
    key = f"{ctx.uid}.{ctx.type.value}"
    key = key.lower()

    url = ctx.urlgen.generate(key, seconds=600)

    return dict(key=key, url=url)

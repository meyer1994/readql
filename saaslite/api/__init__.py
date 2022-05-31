from fastapi import APIRouter

from . import info
from .db import router as router_db
from .csv import router as router_csv
from .upload import router as router_upload

router = APIRouter()
router.include_router(router_db)
router.include_router(router_csv)
router.include_router(router_upload)

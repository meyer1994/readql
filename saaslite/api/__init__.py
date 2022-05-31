from fastapi import FastAPI

from .db import router as router_db
from .csv import router as router_csv
from .upload import router as router_upload

app = FastAPI()
app.include_router(router_db)
app.include_router(router_csv)
app.include_router(router_upload)

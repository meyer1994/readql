import logging

from fastapi import FastAPI

from readql import routes

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


app = FastAPI()
app.include_router(routes.csv)
app.include_router(routes.json)
app.include_router(routes.sqlite)
app.include_router(routes.parquet)
app.include_router(routes.urlgen)

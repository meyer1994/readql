import logging

from fastapi import FastAPI

from saaslite.api import router

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


app = FastAPI()
app.include_router(router)

import json
import logging

from mangum import Mangum

from saaslite import app

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
    logger.info(json.dumps(event))
    mangum = Mangum(app, lifespan='off')
    return mangum(event, context)

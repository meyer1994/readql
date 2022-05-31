import json
import logging

from mangum import Mangum

from saaslite.api import app

logger = logging.getLogger(__name__)


def handler(event, context):
    logger.info(json.dumps(event))
    mangum = Mangum(app, lifespan='off')
    return mangum(event, context)

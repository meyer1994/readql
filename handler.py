import json
import logging

import boto3
from mangum import Mangum

from readql import app

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
    logger.info(json.dumps(event))
    mangum = Mangum(app, lifespan="off")
    return mangum(event, context)


def s3guard(event, context):
    MB_100 = 1024 * 1024 * 100
    s3 = boto3.client("s3")

    for record in event["Records"]:
        if record["s3"]["object"]["size"] > MB_100:
            logger.info("Deleting object %s", record["s3"]["object"]["key"])
            s3.delete_object(
                Bucket=record["s3"]["bucket"]["name"],
                Key=record["s3"]["object"]["key"],
            )

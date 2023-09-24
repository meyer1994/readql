from unittest import TestCase

import boto3
from mypy_boto3_s3 import S3ServiceResource


class S3MinioMixin(TestCase):
    def setUp(self):
        super().setUp()
        
        for bucket in self.s3minio.buckets.all():
            for obj in bucket.objects.all():
                obj.delete()
                obj.wait_until_not_exists()
            bucket.delete()
            bucket.wait_until_not_exists()

    @property
    def s3minio(self) -> S3ServiceResource:
        """
        Copied from:
            https://gist.github.com/heitorlessa/5b709df96ea6ac5ddc600545c0683d3b?permalink_comment_id=4314586#gistcomment-4314586
        """
        return boto3.resource(
            service_name='s3', 
            endpoint_url='http://localhost:9000',
            aws_access_key_id='root',
            aws_secret_access_key='password',
            aws_session_token=None,
            config=boto3.session.Config(signature_version='s3v4'),
            verify=False
        )

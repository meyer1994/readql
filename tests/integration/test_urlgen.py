import time
from pathlib import Path

import httpx

from readql.urlgen import UrlGen

from tests.mixins import S3MinioMixin


DATA_DIR = Path(__file__)
DATA_DIR = DATA_DIR.parent.parent / "data"


class TestUrlGen(S3MinioMixin):
    def setUp(self):
        super().setUp()
        self.bucket = self.s3minio.Bucket("test-bucket-urlgen")
        self.bucket.create()
        self.bucket.wait_until_exists()
        self.client = self.s3minio.meta.client

    def test_url_gen(self):
        urlgen = UrlGen(self.client, "test-bucket-urlgen")
        url = urlgen.generate("TEST_URLGEN", seconds=10)
        response = httpx.put(url, data=b"nice")
        self.assertEqual(response.status_code, 200)

    def test_url_gen_expired(self):
        urlgen = UrlGen(self.client, "test-bucket-urlgen")
        url = urlgen.generate("TEST_URLGEN", seconds=1)
        time.sleep(2)
        response = httpx.put(url, data=b"nice")
        self.assertEqual(response.status_code, 403)

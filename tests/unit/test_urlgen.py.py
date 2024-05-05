from unittest import TestCase
from unittest.mock import Mock

from readql.urlgen import UrlGen


class TestUrlGen(TestCase):
    def test_urlgen(self):
        session = Mock()
        urlgen = UrlGen(session, "TEST_BUCKET")

        urlgen.generate("TEST_KEY", 123)

        urlgen.client.generate_presigned_url(
            Params={"Bucket": "TEST_BUCKET", "Key": "TEST_KEY"},
            ExpiresIn=123,
            ClientMethod="put_object",
        )

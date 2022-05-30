from unittest import TestCase
from unittest.mock import patch, PropertyMock

from saaslite.url import Presigned


class TestPresigned(TestCase):

    @patch('saaslite.url.Presigned.client', new_callable=PropertyMock)
    def test_upload(self, mocked):
        """ Calls `generate_presigned_url` from S3 client """
        presigned = Presigned('bucket-region', 'bucket-name')
        presigned.upload('bucket-key', 123)

        mocked().generate_presigned_url.assert_called_once_with(
            Params={'Bucket': 'bucket-name', 'Key': 'bucket-key'},
            ExpiresIn=123,
            ClientMethod='put_object'
        )

    @patch('saaslite.url.Presigned.client', new_callable=PropertyMock)
    def test_upload_defaults(self, mocked):
        """ Calls `generate_presigned_url` from S3 client, with defaults """
        presigned = Presigned('bucket-region', 'bucket-name')
        presigned.upload('bucket-key')

        mocked().generate_presigned_url.assert_called_once_with(
            Params={'Bucket': 'bucket-name', 'Key': 'bucket-key'},
            ExpiresIn=600,
            ClientMethod='put_object'
        )

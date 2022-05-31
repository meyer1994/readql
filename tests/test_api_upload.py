from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch, Mock

from fastapi import HTTPException

from saaslite.api import upload


class TestApiUpload(IsolatedAsyncioTestCase):

    @patch('saaslite.api.upload.uuid.uuid4')
    @patch('saaslite.api.upload.Presigned')
    async def test_post_sqlite(self, mocked_presigned, mocked_uuid):
        """ Creates a new `uuid.db` object """
        mocked_uuid.return_value = 'abc'
        mocked_uuid.reset_mock()
        mocked_presigned().generate.return_value = 'url'
        mocked_presigned.reset_mock()

        ctx = Mock(kind='sqlite')
        result = await upload.post(ctx)

        expected = {'object_key': 'abc.db', 'upload_url': 'url'}
        self.assertDictEqual(result, expected)

        mocked_uuid.assert_called_once_with()
        mocked_presigned.assert_called_once_with(
            ctx.conf.SAASLITE_S3_BUCKET_REGION,
            ctx.conf.SAASLITE_S3_BUCKET_NAME
        )
        mocked_presigned().generate.assert_called_once_with('abc.db')

    @patch('saaslite.api.upload.uuid.uuid4')
    @patch('saaslite.api.upload.Presigned')
    async def test_post_csv(self, mocked_presigned, mocked_uuid):
        """ Creates a new `uuid.csv` object """
        mocked_uuid.return_value = 'abc'
        mocked_uuid.reset_mock()
        mocked_presigned().generate.return_value = 'url'
        mocked_presigned.reset_mock()

        ctx = Mock(kind='csv')
        result = await upload.post(ctx)

        expected = {'object_key': 'abc.csv', 'upload_url': 'url'}
        self.assertDictEqual(result, expected)

        mocked_uuid.assert_called_once_with()
        mocked_presigned.assert_called_once_with(
            ctx.conf.SAASLITE_S3_BUCKET_REGION,
            ctx.conf.SAASLITE_S3_BUCKET_NAME
        )
        mocked_presigned().generate.assert_called_once_with('abc.csv')

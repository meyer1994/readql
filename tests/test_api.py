from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch, Mock

from fastapi import HTTPException

from saaslite import api


class TestApiQuery(IsolatedAsyncioTestCase):

    @patch('saaslite.api.Select')
    async def test_query(self, mocked):
        """ Calls SQL method if object exists """
        ctx = Mock()
        await api.query(ctx)

        mocked.assert_called_once_with(
            ctx.conf.SAASLITE_S3_BUCKET_REGION,
            ctx.conf.SAASLITE_S3_BUCKET_NAME,
            ctx.filename,
        )
        mocked().exists.assert_called_once_with()
        mocked().sql.assert_called_once_with(ctx.q)

    @patch('saaslite.api.Select')
    async def test_query_not_exists(self, mocked):
        """ Raises HTTPException if object does not  """
        mocked().exists.return_value = False
        mocked.reset_mock()

        ctx = Mock()
        with self.assertRaises(HTTPException):
            await api.query(ctx)

        mocked.assert_called_once_with(
            ctx.conf.SAASLITE_S3_BUCKET_REGION,
            ctx.conf.SAASLITE_S3_BUCKET_NAME,
            ctx.filename,
        )
        mocked().exists.assert_called_once_with()
        mocked().sql.assert_not_called()


class TestApiPost(IsolatedAsyncioTestCase):

    @patch('saaslite.api.uuid.uuid4')
    @patch('saaslite.api.Presigned')
    async def test_post_sqlite(self, mocked_presigned, mocked_uuid):
        """ Creates a new `uuid.db` object """
        mocked_uuid.return_value = 'abc'
        mocked_uuid.reset_mock()
        mocked_presigned().upload.return_value = 'url'
        mocked_presigned.reset_mock()

        ctx = Mock(kind='sqlite')
        result = await api.post(ctx)

        expected = {'object_key': 'abc.db', 'upload_url': 'url'}
        self.assertDictEqual(result, expected)

        mocked_uuid.assert_called_once_with()
        mocked_presigned.assert_called_once_with(
            ctx.conf.SAASLITE_S3_BUCKET_REGION,
            ctx.conf.SAASLITE_S3_BUCKET_NAME
        )
        mocked_presigned().upload.assert_called_once_with('abc.db')

    @patch('saaslite.api.uuid.uuid4')
    @patch('saaslite.api.Presigned')
    async def test_post_csv(self, mocked_presigned, mocked_uuid):
        """ Creates a new `uuid.csv` object """
        mocked_uuid.return_value = 'abc'
        mocked_uuid.reset_mock()
        mocked_presigned().upload.return_value = 'url'
        mocked_presigned.reset_mock()

        ctx = Mock(kind='csv')
        result = await api.post(ctx)

        expected = {'object_key': 'abc.csv', 'upload_url': 'url'}
        self.assertDictEqual(result, expected)

        mocked_uuid.assert_called_once_with()
        mocked_presigned.assert_called_once_with(
            ctx.conf.SAASLITE_S3_BUCKET_REGION,
            ctx.conf.SAASLITE_S3_BUCKET_NAME
        )
        mocked_presigned().upload.assert_called_once_with('abc.csv')

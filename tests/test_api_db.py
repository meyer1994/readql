from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch, Mock

from fastapi import HTTPException

from saaslite.api import db


class TestApiDB(IsolatedAsyncioTestCase):

    @patch('saaslite.api.db.FileDB')
    async def test_get(self, mocked):
        """ Calls SQL method if object exists """
        ctx = Mock()
        ctx.filename = 'abc'

        await db.get(ctx)

        mocked.assert_called_once_with(
            ctx.conf.SAASLITE_S3_BUCKET_REGION,
            ctx.conf.SAASLITE_S3_BUCKET_NAME,
            f'{ctx.filename}.db',
        )
        mocked().exists.assert_called_once_with()
        mocked().sql.assert_called_once_with(ctx.q)

    @patch('saaslite.api.db.FileDB')
    async def test_get_not_exists(self, mocked):
        """ Raises HTTPException if object does not  """
        mocked().exists.return_value = False
        mocked.reset_mock()

        ctx = Mock()
        ctx.filename = 'abc'

        with self.assertRaises(HTTPException):
            await db.get(ctx)

        mocked.assert_called_once_with(
            ctx.conf.SAASLITE_S3_BUCKET_REGION,
            ctx.conf.SAASLITE_S3_BUCKET_NAME,
            f'{ctx.filename}.db',
        )
        mocked().exists.assert_called_once_with()
        mocked().sql.assert_not_called()

from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from saaslite import config


class TestConfig(IsolatedAsyncioTestCase):

    @patch.object(config, 'Config')
    async def test_config(self, mocked):
        """ Calls Config constructor """
        await config.Conf()
        mocked.assert_called_once_with()

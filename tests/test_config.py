from unittest import TestCase
from unittest.mock import patch

from saaslite import config


class TestConfig(TestCase):

    @patch('config.Config')
    async def test_config(self, mocked):
        """ Calls Config constructor """
        config.Conf()
        mocked.assert_called_once_with()

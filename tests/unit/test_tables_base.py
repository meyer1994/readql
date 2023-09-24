from unittest import TestCase
from unittest.mock import Mock, MagicMock

from botocore.exceptions import ClientError

from readql.tables.base import Base


class Table(Base):
    def sql(self, sql: str):
        pass


class TestTablesBase(TestCase):
    def test__sql(self):
        response = {
            'Payload': [
                {'Records': {'Payload': br'{}' + b'\n'}}
            ]
        }

        session = Mock()
        table = Table(session, 'TEST_BUCKET', 'TEST_KEY')
        table.client.select_object_content.return_value = response

        result = table._sql('TEST_SQL', 'TEST_CONFIG')
        result = list(result)

        self.assertEqual(len(result), 1)

        table.client.select_object_content.assert_called_once_with(
            Bucket='TEST_BUCKET',
            Key='TEST_KEY',
            ExpressionType='SQL',
            Expression='TEST_SQL',
            InputSerialization='TEST_CONFIG',
            OutputSerialization={'JSON': {}},
        )

    def test__sql_no_payload(self):
        session = Mock()
        table = Table(session, 'TEST_BUCKET', 'TEST_KEY')
        table.client.select_object_content.return_value = {'whatever': 'val'}

        result = table._sql('TEST_SQL', 'TEST_CONFIG')
        result = list(result)

        self.assertEqual(len(result), 0)

        table.client.select_object_content.assert_called_once_with(
            Bucket='TEST_BUCKET',
            Key='TEST_KEY',
            ExpressionType='SQL',
            Expression='TEST_SQL',
            InputSerialization='TEST_CONFIG',
            OutputSerialization={'JSON': {}},
        )

    def test_exists_true(self):
        session = Mock()
        table = Table(session, 'TEST_BUCKET', 'TEST_KEY')
        result = table.exists()
        self.assertTrue(result)

        table.client.head_object.assert_called_once_with(
            Bucket='TEST_BUCKET',
            Key='TEST_KEY',
        )

    def test_exists_false(self):
        session = Mock()
        table = Table(session, 'TEST_BUCKET', 'TEST_KEY')

        response = {'Error': {'Code': '404'}}
        exception = ClientError(response, 'TEST_ACTION')
        table.client.head_object.side_effect = exception

        result = table.exists()
        self.assertFalse(result)

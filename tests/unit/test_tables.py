from unittest import TestCase
from unittest.mock import Mock, MagicMock

from readql import tables


class TestTables(TestCase):
    def test_csv(self):
        session = Mock()
        csv = tables.CSV(session, 'TEST_BUCKET', 'TEST_KEY')
        csv._sql = MagicMock()

        result = csv.sql(
            sql='TEST_SQL', 
            delimiter='TEST_DELIMITER', 
            header='TEST_HEADER', 
            compression='TEST_COMPRESSION',
        )
        list(result)

        csv._sql.assert_called_once_with('TEST_SQL', {
            'CompressionType': 'TEST_COMPRESSION',
            'CSV': {
                'FileHeaderInfo': 'TEST_HEADER', 
                'FieldDelimiter': 'TEST_DELIMITER',
            },
        })

    def test_json(self):
        session = Mock()
        json = tables.JSON(session, 'TEST_BUCKET', 'TEST_KEY')
        json._sql = MagicMock()

        result = json.sql(
            sql='TEST_SQL', 
            type='TEST_TYPE',
            compression='TEST_COMPRESSION',
        )
        list(result)

        json._sql.assert_called_once_with('TEST_SQL', {
            'JSON': {'Type': 'TEST_TYPE'},
            'CompressionType': 'TEST_COMPRESSION',
        })

    def test_parquet(self):
        session = Mock()
        parquet = tables.Parquet(session, 'TEST_BUCKET', 'TEST_KEY')
        parquet._sql = MagicMock()

        result = parquet.sql(sql='TEST_SQL')
        list(result)

        parquet._sql.assert_called_once_with('TEST_SQL', {'Parquet': {}})
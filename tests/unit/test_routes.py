from unittest import TestCase
from unittest.mock import patch, Mock

from readql.errors import FileNotFoundError
from readql.routes.csv import csv
from readql.routes.json import json
from readql.routes.sqlite import sqlite
from readql.routes.parquet import parquet


class TestRoutes(TestCase):
    @patch('readql.routes.csv.CSV')
    def test_csv_exists(self, CSV):
        ctx = Mock()
        ctx.q = 'TEST_SQL'
        ctx.key = 'TEST_KEY'
        ctx.header.value = 'TEST_HEADER'
        ctx.delimiter = 'TEST_DELIMITER'
        ctx.compression.value = 'TEST_COMPRESSION'
        ctx.config.READQL_S3_BUCKET_NAME = 'TEST_BUCKET'
        CSV.return_value.exists.return_value = True

        csv(ctx)

        CSV.assert_called_once_with(
            session=ctx.session,
            bucket='TEST_BUCKET',
            key='TEST_KEY.csv'
        )

        table = CSV.return_value
        table.exists.assert_called_once_with()
        table.sql.assert_called_once_with(
            sql='TEST_SQL',
            delimiter='TEST_DELIMITER',
            header='TEST_HEADER',
            compression='TEST_COMPRESSION',
        )

    @patch('readql.routes.json.JSON')
    def test_json_exists(self, JSON):
        ctx = Mock()
        ctx.q = 'TEST_SQL'
        ctx.key = 'TEST_KEY'
        ctx.type.value = 'TEST_TYPE'
        ctx.compression.value = 'TEST_COMPRESSION'
        ctx.config.READQL_S3_BUCKET_NAME = 'TEST_BUCKET'
        JSON.return_value.exists.return_value = True

        json(ctx)

        JSON.assert_called_once_with(
            session=ctx.session,
            bucket='TEST_BUCKET',
            key='TEST_KEY.json'
        )

        table = JSON.return_value
        table.exists.assert_called_once_with()
        table.sql.assert_called_once_with(
            sql='TEST_SQL',
            type='TEST_TYPE',
            compression='TEST_COMPRESSION',
        )

    @patch('readql.routes.parquet.Parquet')
    def test_parquet_exists(self, Parquet):
        ctx = Mock()
        ctx.q = 'TEST_SQL'
        ctx.key = 'TEST_KEY'
        ctx.compression.value = 'TEST_COMPRESSION'
        ctx.config.READQL_S3_BUCKET_NAME = 'TEST_BUCKET'
        Parquet.return_value.exists.return_value = True

        parquet(ctx)

        Parquet.assert_called_once_with(
            session=ctx.session,
            bucket='TEST_BUCKET',
            key='TEST_KEY.parquet'
        )

        table = Parquet.return_value
        table.exists.assert_called_once_with()
        table.sql.assert_called_once_with(
            sql='TEST_SQL',
            compression='TEST_COMPRESSION',
        )

    @patch('readql.routes.sqlite.Sqlite')
    def test_parquet_sqlite(self, Sqlite):
        ctx = Mock()
        ctx.q = 'TEST_SQL'
        ctx.key = 'TEST_KEY'
        ctx.compression.value = 'TEST_COMPRESSION'
        ctx.config.READQL_S3_BUCKET_NAME = 'TEST_BUCKET'
        Sqlite.return_value.exists.return_value = True

        sqlite(ctx)

        Sqlite.assert_called_once_with(
            session=ctx.session,
            bucket='TEST_BUCKET',
            key='TEST_KEY.sqlite'
        )

        table = Sqlite.return_value
        table.exists.assert_called_once_with()
        table.sql.assert_called_once_with(sql='TEST_SQL')

    @patch('readql.routes.csv.CSV')
    def test_csv_not_exists(self, CSV):
        ctx = Mock()
        ctx.q = 'TEST_SQL'
        ctx.key = 'TEST_KEY'
        ctx.config.READQL_S3_BUCKET_NAME = 'TEST_BUCKET'
        CSV.return_value.exists.return_value = False

        with self.assertRaises(FileNotFoundError):
            csv(ctx)

        CSV.assert_called_once_with(
            session=ctx.session,
            bucket='TEST_BUCKET',
            key='TEST_KEY.csv'
        )

    @patch('readql.routes.json.JSON')
    def test_json_not_exists(self, JSON):
        ctx = Mock()
        ctx.q = 'TEST_SQL'
        ctx.key = 'TEST_KEY'
        ctx.config.READQL_S3_BUCKET_NAME = 'TEST_BUCKET'
        JSON.return_value.exists.return_value = False

        with self.assertRaises(FileNotFoundError):
            json(ctx)

        JSON.assert_called_once_with(
            session=ctx.session,
            bucket='TEST_BUCKET',
            key='TEST_KEY.json'
        )

    @patch('readql.routes.parquet.Parquet')
    def test_parquet_not_exists(self, Parquet):
        ctx = Mock()
        ctx.q = 'TEST_SQL'
        ctx.key = 'TEST_KEY'
        ctx.config.READQL_S3_BUCKET_NAME = 'TEST_BUCKET'
        Parquet.return_value.exists.return_value = False

        with self.assertRaises(FileNotFoundError):
            parquet(ctx)

        Parquet.assert_called_once_with(
            session=ctx.session,
            bucket='TEST_BUCKET',
            key='TEST_KEY.parquet'
        )

    @patch('readql.routes.sqlite.Sqlite')
    def test_parquet_not_exists(self, Sqlite):
        ctx = Mock()
        ctx.q = 'TEST_SQL'
        ctx.key = 'TEST_KEY'
        ctx.config.READQL_S3_BUCKET_NAME = 'TEST_BUCKET'
        Sqlite.return_value.exists.return_value = False

        with self.assertRaises(FileNotFoundError):
            sqlite(ctx)

        Sqlite.assert_called_once_with(
            session=ctx.session,
            bucket='TEST_BUCKET',
            key='TEST_KEY.sqlite'
        )

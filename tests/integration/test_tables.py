from pathlib import Path

from readql.tables.csv import CSV
from readql.tables.json import JSON
from readql.tables.sqlite import Sqlite
from readql.tables.parquet import Parquet

from tests.mixins import S3MinioMixin


DATA_DIR = Path(__file__)
DATA_DIR = DATA_DIR.parent.parent / 'data'


class TestTables(S3MinioMixin):
    def setUp(self):
        super().setUp()
        self.bucket = self.s3minio.Bucket('test-bucket-tables')
        self.bucket.create()
        self.bucket.wait_until_exists()
        self.client = self.s3minio.meta.client

    def test_table_csv(self):
        self.bucket.upload_file(
            Filename=DATA_DIR / 'test.csv',
            Key='TEST_KEY_CSV'
        )

        table = CSV(self.client, self.bucket.name, 'TEST_KEY_CSV')

        result = table.sql(
            sql='SELECT * FROM s3Object',
            header='USE',
            compression='NONE',
            delimiter=',',
        )
        result = list(result)

        self.assertEqual(len(result), 1)
        self.assertDictEqual(result[0], {
            'a': '1',
            'b': '2',
            'c': '3',
        })

    def test_table_csv_gzip(self):
        self.bucket.upload_file(
            Filename=DATA_DIR / 'test.csv.gz',
            Key='TEST_KEY_CSV_GZIP'
        )

        table = CSV(self.client, self.bucket.name, 'TEST_KEY_CSV_GZIP')

        result = table.sql(
            sql='SELECT * FROM s3Object',
            header='USE',
            compression='GZIP',
            delimiter=',',
        )
        result = list(result)

        self.assertEqual(len(result), 1)
        self.assertDictEqual(result[0], {
            'a': '1',
            'b': '2',
            'c': '3',
        })

    def test_table_csv_bzip2(self):
        self.bucket.upload_file(
            Filename=DATA_DIR / 'test.csv.bz2',
            Key='TEST_KEY_CSV_BZIP2'
        )

        table = CSV(self.client, self.bucket.name, 'TEST_KEY_CSV_BZIP2')

        result = table.sql(
            sql='SELECT * FROM s3Object',
            header='USE',
            compression='BZIP2',
            delimiter=',',
        )
        result = list(result)

        self.assertEqual(len(result), 1)
        self.assertDictEqual(result[0], {
            'a': '1',
            'b': '2',
            'c': '3',
        })

    def test_table_json(self):
        self.bucket.upload_file(
            Filename=DATA_DIR / 'test.json',
            Key='TEST_KEY_JSON'
        )

        table = JSON(self.client, self.bucket.name, 'TEST_KEY_JSON')

        result = table.sql(
            sql='SELECT * FROM s3Object',
            compression='NONE',
            type='DOCUMENT',
        )
        result = list(result)

        self.assertEqual(len(result), 1)
        self.assertDictEqual(result[0], {
            'a': 1,
            'b': 2,
            'c': 3,
        })

    def test_table_json_gzip(self):
        self.bucket.upload_file(
            Filename=DATA_DIR / 'test.json.gz',
            Key='TEST_KEY_JSON_GZIP'
        )

        table = JSON(self.client, self.bucket.name, 'TEST_KEY_JSON_GZIP')

        result = table.sql(
            sql='SELECT * FROM s3Object',
            compression='GZIP',
            type='DOCUMENT',
        )
        result = list(result)

        self.assertEqual(len(result), 1)
        self.assertDictEqual(result[0], {
            'a': 1,
            'b': 2,
            'c': 3,
        })

    def test_table_json_bzip2(self):
        self.bucket.upload_file(
            Filename=DATA_DIR / 'test.json.bz2',
            Key='TEST_KEY_JSON_BZIP2'
        )

        table = JSON(self.client, self.bucket.name, 'TEST_KEY_JSON_BZIP2')

        result = table.sql(
            sql='SELECT * FROM s3Object',
            compression='BZIP2',
            type='DOCUMENT',
        )
        result = list(result)

        self.assertEqual(len(result), 1)
        self.assertDictEqual(result[0], {
            'a': 1,
            'b': 2,
            'c': 3,
        })

    def test_table_parquet(self):
        self.bucket.upload_file(
            Filename=DATA_DIR / 'test.parquet',
            Key='TEST_KEY_PARQUET'
        )

        table = Parquet(self.client, self.bucket.name, 'TEST_KEY_PARQUET')

        result = table.sql(sql='SELECT * FROM s3Object')
        result = list(result)

        self.assertEqual(len(result), 1)
        self.assertDictEqual(result[0], {
            'a': 1,
            'b': 2,
            'c': 3,
        })

    def test_table_sqlite(self):
        self.bucket.upload_file(
            Filename=DATA_DIR / 'test.sqlite',
            Key='TEST_KEY_SQLITE'
        )

        table = Sqlite(self.client, self.bucket.name, 'TEST_KEY_SQLITE')

        result = table.sql(sql='SELECT * FROM test')
        result = list(result)

        self.assertEqual(len(result), 1)
        self.assertDictEqual(result[0], {
            'a': 1,
            'b': 2,
            'c': 3,
        })

    def test_table_exists(self):
        table = Sqlite(self.client, self.bucket.name, 'TEST_KEY_SQLITE')

        result = table.exists()
        self.assertFalse(result)

        self.bucket.upload_file(
            Filename=DATA_DIR / 'test.sqlite',
            Key='TEST_KEY_SQLITE'
        )

        result = table.exists()
        self.assertTrue(result)
from unittest import TestCase
from unittest.mock import MagicMock, patch, Mock, ANY

from readql import tables


class TestTablesSqlite(TestCase):
    @patch.object(tables.sqlite, "apsw")
    @patch.object(tables.sqlite, "S3VFS")
    def test_sqlite(self, S3VF, apsw):
        session = Mock()
        sqlite = tables.Sqlite(session, "TEST_BUCKET", "TEST_KEY")

        result = sqlite.sql("TEST_SQL")
        list(result)

        S3VF.assert_called_once_with(sqlite.client, sqlite.bucket)
        apsw.Connection.assert_called_once_with(
            "file:/TEST_KEY",
            flags=sqlite.FLAGS,
            vfs=S3VF.return_value.name,
        )
        apsw.Connection.return_value.cursor.return_value.execute.assert_called_once_with(
            "TEST_SQL"
        )

    def test_file_size(self):
        client = MagicMock()
        name = MagicMock()
        s3vfsfile = tables.sqlite.S3VFSFile(
            name=name, flags=1, client=client, bucket="TEST_BUCKET"
        )

        s3vfsfile.xFileSize()

        client.head_object.assert_called_once_with(
            Bucket="TEST_BUCKET",
            Key=ANY,
        )

    def test_file_read(self):
        client = MagicMock()
        name = MagicMock()
        s3vfsfile = tables.sqlite.S3VFSFile(
            name=name, flags=1, client=client, bucket="TEST_BUCKET"
        )

        s3vfsfile.xRead(10, 50)

        client.get_object.assert_called_once_with(
            Bucket="TEST_BUCKET", Key=ANY, Range="bytes=50-60"
        )

import uuid
from unittest import TestCase
from unittest.mock import patch, Mock, ANY

from readql.models import FileType
from readql.routes.urlgen import urlgen


class TestRoutesUrlGen(TestCase):
    def test_urlgen(self):
        ctx = Mock()
        ctx.uid = uuid.uuid4()
        ctx.type = FileType.CSV

        result = urlgen(ctx)

        self.assertDictEqual(result, {'key': f'{ctx.uid}.csv', 'url': ANY})

        ctx.urlgen.generate\
            .assert_called_once_with(f'{ctx.uid}.csv', seconds=600)

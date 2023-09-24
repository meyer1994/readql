import logging
from typing import Iterable

from readql.tables.base import Base


logger = logging.getLogger(__name__)


class CSV(Base):
    def sql(
        self, 
        sql: str, 
        delimiter: str, 
        header: str, 
        compression: str,
    ) -> Iterable[dict]:
        yield from self._sql(sql, {
            'CompressionType': compression,
            'CSV': {'FileHeaderInfo': header, 'FieldDelimiter': delimiter},
        })

import logging
from typing import Iterable

from readql.tables.base import Base


logger = logging.getLogger(__name__)


class JSON(Base):
    def sql(self, sql: str, type: str, compression: str) -> Iterable[dict]:
        yield from self._sql(sql, {
            'JSON': {'Type': type},
            'CompressionType': compression,
        })

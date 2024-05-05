import logging
from typing import Iterable

from readql.tables.base import Base

logger = logging.getLogger(__name__)


class Parquet(Base):
    def sql(self, sql: str) -> Iterable[dict]:
        yield from self._sql(sql, {"Parquet": {}})

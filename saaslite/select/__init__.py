from .csv import SelectCSV
from .base import SelectBase
from .sqlite import SelectSQLite


def Select(bucket_region: str, bucket_name: str, bucket_key: str) -> SelectBase:
    if bucket_key.endswith('.db'):
        return SelectSQLite(bucket_region, bucket_name, bucket_key)

    if bucket_key.endswith('.csv'):
        return SelectCSV(bucket_region, bucket_name, bucket_key)

    raise ValueError('Not a valid extension')

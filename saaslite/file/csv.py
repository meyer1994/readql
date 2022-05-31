import json
import logging

from saaslite.file.base import FileBase

logger = logging.getLogger(__name__)


class FileCSV(FileBase):
    def _input_serial(self, delimiter: str, header: str) -> dict:
        return {
            'CompressionType': 'NONE',
            'CSV': {
                'FileHeaderInfo': header,
                'FieldDelimiter': delimiter
            }
        }

    def sql(self, sql: str, delimiter: str, header: str) -> list:
        input_serial = self._input_serial(delimiter, header)

        logger.info('Querying s3://%s/%s: %s', self.bucket_name, self.bucket_key, sql)  # noqa
        response = self.client.select_object_content(
            Bucket=self.bucket_name,
            Key=self.bucket_key,
            ExpressionType='SQL',
            Expression=sql,
            InputSerialization=input_serial,
            OutputSerialization={'JSON': {}},
        )

        if 'Payload' not in response:
            logger.info('No payload in response %s', response)
            return

        for payload in response['Payload']:
            if 'Records' not in payload:
                logger.info('No records in payload: %s', payload)
                continue

            records = payload['Records']['Payload']
            records = records.split(b'\n')
            records.pop()  # last one, after splitting, is always empty

            for record in records:
                yield json.loads(record)

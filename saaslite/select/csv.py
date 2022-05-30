import json

from saaslite.select.base import SelectBase


class SelectCSV(SelectBase):
    OUTPUT_SERIAL = {'JSON': {} }
    INPUT_SERIAL = {
        'CompressionType': 'NONE',
        'CSV': { 'FileHeaderInfo': 'None' },
    }

    def sql(self, sql: str) -> list:
        response = self.client.select_object_content(
            Bucket=self.bucket_name,
            Key=self.bucket_key,
            ExpressionType='SQL',
            Expression=sql,
            InputSerialization=self.INPUT_SERIAL,
            OutputSerialization=self.OUTPUT_SERIAL,
        )

        if 'Payload' not in response:
            return

        for payload in response['Payload']:
            if 'Records' not in payload:
                continue

            records = payload['Records']['Payload']
            records = records.split(b'\n')
            records.pop()  # last one, after splitting, is always empty

            for record in records:
                yield json.loads(record)

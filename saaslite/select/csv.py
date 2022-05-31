import json
import copy

from saaslite.select.base import SelectBase


class SelectCSV(SelectBase):
    OUTPUT_SERIAL = {'JSON': {} }
    INPUT_SERIAL = {
        'CompressionType': 'NONE',
        'CSV': {
            'FileHeaderInfo': 'None',
            'FieldDelimiter': ','
        },
    }

    def sql(self, sql: str, delimiter: str = ',', header: bool = False) -> list:
        input_serial = copy.deepcopy(self.INPUT_SERIAL)
        input_serial['CSV']['FieldDelimiter'] = delimiter
        input_serial['CSV']['FileHeaderInfo'] = 'USE' if header else 'NONE'
        print(input_serial)

        response = self.client.select_object_content(
            Bucket=self.bucket_name,
            Key=self.bucket_key,
            ExpressionType='SQL',
            Expression=sql,
            InputSerialization=input_serial,
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

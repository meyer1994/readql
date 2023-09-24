import boto3

client = boto3.client(
            service_name='s3', 
            endpoint_url='http://localhost:9000',
            aws_access_key_id='root',
            aws_secret_access_key='password',
            aws_session_token=None,
            config=boto3.session.Config(signature_version='s3v4'),
            verify=False
        )


response = client.select_object_content(
    Bucket='test-bucket-tables',
    Key='TEST_KEY_PARQUET_GZIP',
    ExpressionType='SQL',
    Expression='SELECT * FROM s3Object',
    InputSerialization={'Parquet': {}},
    OutputSerialization={'JSON': {}},
)

from pprint import pp
pp(response)
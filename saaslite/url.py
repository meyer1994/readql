import boto3


class Presigned(object):
    def __init__(self, bucket_region: str, bucket_name: str):
        super(Presigned, self).__init__()
        self.bucket_region = bucket_region
        self.bucket_name = bucket_name

    @property
    def client(self):
        return boto3.client('s3', region_name=self.bucket_region)

    def upload(self, key: str, seconds: int = 600) -> str:
        params = { 'Bucket': self.bucket_name, 'Key': key }
        return self.client.generate_presigned_url(
            Params=params,
            ExpiresIn=seconds,
            ClientMethod='put_object'
        )

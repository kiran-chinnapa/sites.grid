import boto3
s3_resource = boto3.resource('s3')

from boto3.s3.transfer import TransferConfig
config = TransferConfig(multipart_threshold=1024 * 25,
                        max_concurrency=10,
                        multipart_chunksize=1024 * 1000,
                        use_threads=True)


bucket_name = 'bigparser-largefile-testing'
def multipart_upload_boto3():

    file_path = '/home/ubuntu/kiran/sites.grid.csv'
    key = 'sites.grid.csv'

    s3_resource.Object(bucket_name, key).upload_file(file_path,
                            ExtraArgs={'ContentType': 'text/pdf'},
                            Config=config
                            )

multipart_upload_boto3()

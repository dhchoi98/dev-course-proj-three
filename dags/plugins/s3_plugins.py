from airflow.providers.amazon.aws.hooks.s3 import S3Hook


import logging

def upload_to_s3(s3_key, bucket_name, local_files_to_upload, replace):
    s3_hook = S3Hook('aws_conn_id')
        
    for file in local_files_to_upload:
        logging.info("Saving {} to {} in S3".format(file, s3_key))
        s3_hook.load_file(
            filename=file,
            key=s3_key,
            bucket_name=bucket_name,
            replace=replace
        )

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import pdb

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    pdb.set_trace()  # 디버깅 모드 진입
    hook = S3Hook('aws_conn_id')
    
    files = hook.list_keys(bucket_name)
    if files is not None:
        print(f"Bucket {bucket_name} contains: {files}")
    else:
        print(f"Bucket {bucket_name} is empty or not accessible.")
    
    hook.load_file(filename=filename,
                   key=key,
                   bucket_name=bucket_name,
                   replace=True
                   )


with DAG('upload_to_s3',
         schedule_interval=None,
         start_date=datetime(2022, 1, 1),
         catchup=False
         ) as dag:
    upload = PythonOperator(task_id='upload',
                            python_callable=upload_to_s3,
                            op_kwargs={
                                'filename': '/opt/airflow/data/test.csv',
                                'key': 'dataSource/test.csv',
                                'bucket_name': 'dust-dag'
                            })
    upload
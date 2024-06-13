from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
import requests
import datetime
import os

import pdb

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# DAG 정의
with DAG(
    'api_to_s3_to_redshift',
    default_args=default_args,
    description='Fetch data from API, upload to S3, and load into Redshift',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # 1. API 호출하여 데이터프레임 형태로 변환하는 함수
    def fetch_api_data():
        # 현재 날짜를 YYYYMMDDHH 형식으로 얻기 (예: 1시간 전 데이터 요청)
        current_datetime = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y%m%d%H00")
        
        # API URL (json 형식으로 요청)
        api_url = f"http://openapi.seoul.go.kr:8088/4b746a725579756e35386955445a73/json/TimeAverageCityAir/1/100/{current_datetime}"
        response = requests.get(api_url)
        try:
            response.raise_for_status()  # HTTP 응답 상태 코드 확인
            data = response.json()
        except requests.exceptions.HTTPError as http_err:
            raise ValueError(f"HTTP error occurred: {http_err}")
        except requests.exceptions.RequestException as req_err:
            raise ValueError(f"Request exception occurred: {req_err}")
        except ValueError as json_err:
            raise ValueError(f"JSON decode error: {json_err}")
        
        if "TimeAverageCityAir" not in data or "row" not in data["TimeAverageCityAir"]:
            raise ValueError("No data returned from API")
        
        items = data["TimeAverageCityAir"]["row"]
        if not items:
            raise ValueError("No data available for the requested date and time.")
        
        df = pd.DataFrame(items)

        # 컬럼명을 ERD의 영어 이름으로 변경
        df.columns = [
            'date', 'region_code', 'region_name', 'office_code', 'office_name',
            'dust_1h', 'dust_24h', 'ultradust', 'O3', 'NO2', 'CO', 'SO2'
        ]
        # 데이터프레임을 UTF-8 인코딩으로 CSV 형식의 문자열로 변환
        csv_data = df.to_csv(index=False, encoding='utf-8-sig')
        
        # pdb.set_trace()
        
        # 현재 작업 디렉토리를 사용하여 파일 저장
        file_path = os.path.join(os.getcwd(), 'api_data.csv')
        with open(file_path, 'w', encoding='utf-8-sig') as f:
            f.write(csv_data)
        
        return file_path

    fetch_data = PythonOperator(
        task_id='fetch_api_data',
        python_callable=fetch_api_data,
    )

    # 2. 데이터프레임 형태로 변환한 데이터를 S3에 업로드
    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id='upload_to_s3',
        filename="{{ task_instance.xcom_pull(task_ids='fetch_api_data') }}",
        dest_bucket='dust-dag',
        dest_key='dataSource/api_data.csv',
        aws_conn_id='aws_s3',
        replace=True  # 파일이 이미 존재하는 경우 덮어쓰기
    )

    # 3. S3 데이터를 Redshift 테이블에 적재
    load_to_redshift = S3ToRedshiftOperator(
        task_id='load_to_redshift',
        schema='yusuyeon678',
        table='raw_data_test_youngjun',
        s3_bucket='dust-dag',
        s3_key='dataSource/api_data.csv',
        copy_options=['csv', 'IGNOREHEADER 1'],
        redshift_conn_id='redshift_test_dev',
        aws_conn_id='aws_s3',
    )

    # Task 순서 정의
    fetch_data >> upload_to_s3 >> load_to_redshift
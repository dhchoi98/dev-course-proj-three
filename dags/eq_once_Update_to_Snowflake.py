from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import requests
import xml.etree.ElementTree as ET
import pandas as pd

from datetime import datetime
from datetime import timedelta

from plugins import s3_plugins
from apis import earthquake_apis


def makefile(df, filename):
    df.to_csv(filename, index=False)


def apidata_to_s3(**context):
    # 기본 설정값 가져오기
    eq_base_url = Variable.get("eq_base_url")
    auth_key = Variable.get("eq_auth_key")
    data_dir = Variable.get("DATA_DIR")
    
    # 실행 날짜 설정 1년 단위
    base_date = context["execution_date"] 
    sdate = (base_date - timedelta(1825)).strftime('%Y%m%d')
    edate = base_date.strftime('%Y%m%d')
    
    # 파일명 설정
    filename = "eq_rawdata"
    s3_folder = 'earthquake/'
    s3_key = s3_folder+f"eq_once_{sdate}_{edate}.csv"

    # 데이터 수집 및 처리
    df = earthquake_apis.rtn_eq_dataframe(
        earthquake_apis.get_api_xmldata(eq_base_url, sdate, edate, auth_key)
    )

    # 로컬 파일 저장
    local_file_path = f"{data_dir}{filename}.csv"
    makefile(df, local_file_path)  
    bucket_name = Variable.get("de06-8team-thirdproject")

    # S3 업로드
    s3_plugins.upload_to_s3(
        s3_key,
        bucket_name,
        [local_file_path],
        replace=True,
    )

dag = DAG(
    dag_id = 'eq_once_Update_to_Snowflake',
    start_date = datetime(2025,6,4), # 날짜가 미래인 경우 실행이 안됨
    schedule = None,  # 적당히 조절
    max_active_runs = 1,
    max_active_tasks = 1,
    catchup = False,
    # default_args = {
    #     'retries': 1,
    #     'retry_delay': timedelta(minutes=3),
    # } 
)

update_daily_eq_data = PythonOperator(
    task_id='update_once_eq_data',
    python_callable=apidata_to_s3,
    dag=dag
)

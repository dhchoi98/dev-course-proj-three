from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import snowflake.connector


import requests
import xml.etree.ElementTree as ET
import pandas as pd

from datetime import datetime
from datetime import timedelta

from plugins import s3_plugins
from apis import earthquake_apis

#Snoflake connection 정보
snow_conn = BaseHook.get_connection('snowflake_conn')

def makefile(df, filename):
    df.to_csv(filename, index=False)


def apidata_to_s3(**context):
    # 기본 설정값 가져오기
    eq_base_url = Variable.get("eq_base_url")
    auth_key = Variable.get("eq_auth_key")
    data_dir = Variable.get("DATA_DIR")
    
    # 실행 날짜 설정 5년 단위
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
    
    context['task_instance'].xcom_push(key='sdate', value=sdate)
    context['task_instance'].xcom_push(key='edate', value=edate)

def s3_to_snowflake(**context):
    # 연결 정보
    extra = snow_conn.extra_dejson
    
     # XCom에서 값 가져오기
    sdate = context['task_instance'].xcom_pull(task_ids='update_once_eq_data', key='sdate')
    edate = context['task_instance'].xcom_pull(task_ids='update_once_eq_data', key='edate')
    
    
    conn = snowflake.connector.connect(
        user=snow_conn.login,
        password=snow_conn.password,  
        account=extra.get('account'),  
        warehouse=extra.get('warehouse'),
        database=extra.get('database'),
        schema=snow_conn.schema,
        role=extra.get('role')
    )
    cursor = conn.cursor()

    # COPY INTO 쿼리 실행
    copy_sql = f"""
        COPY INTO raw_eq_world
        FROM @my_s3_stage
        FILES = ('eq_once_{sdate}_{edate}.csv');
    """
    cursor.execute(copy_sql)
    print("COPY INTO 쿼리 실행 완료")
    for row in cursor:
        print("결과 : ", row[0])
    # 연결 종료
    cursor.close()
    conn.close()

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

update_once_eq_data = PythonOperator(
    task_id='update_once_eq_data',
    python_callable=apidata_to_s3,
    dag=dag
)

update_once_S3_to_snowflake = PythonOperator(
    task_id='update_once_S3_to_snowflake',
    python_callable=s3_to_snowflake,
    dag=dag
)

update_once_eq_data >> update_once_S3_to_snowflake
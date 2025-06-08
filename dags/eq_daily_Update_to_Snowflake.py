from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import S3ToSnowflakeOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import snowflake.connector

import requests
import xml.etree.ElementTree as ET
import pandas as pd
import os
import logging

from datetime import datetime
from datetime import timedelta

from plugins import s3_plugins
from plugins import send_slack_plugins
from apis import earthquake_apis

#Snoflake connection 정보
snow_conn = BaseHook.get_connection('snowflake_conn')

def makefile(df, filename):
    df.to_csv(filename, index=False)


def apidata_to_s3(**context):
    try:
        # 기본 설정값 가져오기
        eq_base_url = Variable.get("eq_base_url")
        auth_key = Variable.get("eq_auth_key")
        data_dir = Variable.get("DATA_DIR")
        
        # 실행 날짜 설정
        exe_date = context["execution_date"] 
        basedate = exe_date.strftime('%Y%m%d')
        
        print("basedate : " + basedate)
        
        # 파일명 설정
        filename = "eq_rawdata"
        s3_folder = 'earthquake/'
        s3_key = s3_folder+f"eq_daily_{basedate}.csv"
        local_file_path = f"{data_dir}{filename}.csv"

        print('s3_key : ' + s3_key)
        
        # 데이터 수집 및 처리
        try:
            df = earthquake_apis.rtn_eq_dataframe(
                earthquake_apis.get_api_xmldata(eq_base_url, basedate, basedate, auth_key)
            )
        except:
            error_msg = f"데이터 수집 중 오류 발생: {str(e)}"
            # send_slack_plugins.send_slack_notification(context, error_msg, is_error=True)
            raise
            
        # 로컬 파일 저장
        try:
            makefile(df, local_file_path)
        except Exception as e:
            error_msg = f"로컬 파일 저장 중 오류 발생: {str(e)}"
            # send_slack_plugins.send_slack_notification(context, error_msg, is_error=True)
            raise
        
        # S3 업로드
        try:
            bucket_name = Variable.get("de06-8team-thirdproject")
            s3_plugins.upload_to_s3(
                s3_key,
                bucket_name,
                [local_file_path],
                replace=True,
            )
        except Exception as e:
            error_msg = f"S3 업로드 중 오류 발생: {str(e)}"
            # send_slack_plugins.send_slack_notification(context, error_msg, is_error=True)
            raise
        
        # 성공 메시지 전송
        success_msg = f"지진 API 데이터 처리 완료: {basedate}"
        # send_slack_plugins.send_slack_notification(context, success_msg, is_error=False)
        context['task_instance'].xcom_push(key='s3_key', value=s3_key)
        context['task_instance'].xcom_push(key='basedate', value=basedate)
    
    except Exception as e:
        logging.error(f"전체 프로세스 중 오류 발생: {str(e)}")
        raise

    finally:
        # 임시 파일 정리
        if local_file_path and os.path.exists(local_file_path):
            try:
                os.remove(local_file_path)
                logging.info(f"임시 파일 삭제 완료: {local_file_path}")
            except Exception as e:
                logging.error(f"임시 파일 삭제 중 오류 발생: {str(e)}")

def s3_to_snowflake(**context):
    # 연결 정보
    extra = snow_conn.extra_dejson
    
     # XCom에서 값 가져오기
    basedate = context['task_instance'].xcom_pull(task_ids='update_daily_eq_to_S3', key='basedate')
    
    
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

    # DELETE 쿼리 실행
    delete_sql = f"""
        DELETE FROM raw_eq_world 
        WHERE TO_TIMESTAMP(EQDATE, 'YYYYMMDDHH24MISS') 
        BETWEEN TO_TIMESTAMP('{basedate}', 'YYYYMMDD') 
        AND TO_TIMESTAMP('{basedate}', 'YYYYMMDD') + INTERVAL '1 DAY' - INTERVAL '1 SECOND';
    """
    cursor.execute(delete_sql)
    print("DELETE 쿼리 실행 완료")

    # COPY INTO 쿼리 실행
    copy_sql = f"""
        COPY INTO raw_eq_world
        FROM @my_s3_stage
        FILES = ('eq_daily_{basedate}.csv')
        FORCE=TRUE;
    """
    cursor.execute(copy_sql)
    print("COPY INTO 쿼리 실행 완료")
    for row in cursor:
        print("결과 : ", row[0])
    # 연결 종료
    cursor.close()
    conn.close()

dag = DAG(
    dag_id = 'eq_daily_Update_to_Snowflake',
    start_date = datetime(2025,6,4), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 9 * * *',  # 적당히 조절
    max_active_runs = 1,
    max_active_tasks = 1,
    catchup = False,
    # default_args = {
    #     'retries': 1,
    #     'retry_delay': timedelta(minutes=3),
    # } 
)

update_daily_eq_to_S3 = PythonOperator(
    task_id='update_daily_eq_to_S3',
    python_callable=apidata_to_s3,
    dag=dag
)

# update_daily_S3_to_snowflake = S3ToSnowflakeOperator(
#     task_id='update_daily_S3_to_snowflake',
#     snowflake_conn_id='snowflake_conn_id',
#     s3_keys=["{{ task_instance.xcom_pull(task_ids='update_daily_eq_to_S3', key='s3_key') }}"],
#     # table=Variable.get('RAW_EQ_WORLD'),
#     table='RAW_EQ_WORLD',
#     # schema=snow_conn.extra_dejson.get('schema'),
#     schema='RAW_DATA',
#     stage='MY_S3_STAGE',
#     file_format="(type = 'CSV',field_delimiter = ',')",
#     dag=dag,
# )

update_daily_S3_to_snowflake = PythonOperator(
    task_id='update_daily_S3_to_snowflake',
    python_callable=s3_to_snowflake,
    dag=dag
)

update_daily_eq_to_S3 >> update_daily_S3_to_snowflake
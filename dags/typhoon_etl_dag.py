from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import snowflake.connector
import pandas as pd
import os
import logging
from datetime import datetime, timedelta
import sys

# Add the apis directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), "apis"))
from apis.typhoon_utils import fetch_typhoon_data, process_typhoon_data

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Snowflake connection 정보
snow_conn = BaseHook.get_connection('snowflake_conn')

def makefile(df, filename):
    # NaN 값을 None으로 변환
    df = df.replace({pd.NA: None, pd.NaT: None})
    df = df.where(pd.notnull(df), None)
    # CSV 저장 시 NULL 값 처리
    df.to_csv(filename, index=False, na_rep='')

def apidata_to_s3(**context):
    try:
        data_dir = Variable.get("DATA_DIR")
        exe_date = context["execution_date"]
        end_date = exe_date
        start_date = exe_date - timedelta(days=3)
        basedate = end_date.strftime('%Y%m%d')
        
        # 파일명 설정
        filename = "typhoon_rawdata"
        s3_folder = 'typhoon/'
        s3_key = s3_folder + f"typhoon_daily_{basedate}.csv"
        local_file_path = f"{data_dir}{filename}.csv"

        # 3일치 데이터 수집
        dates = []
        current = start_date
        while current <= end_date:
            for hour in [0, 6, 12, 18]:
                dt = current.replace(hour=hour, minute=0)
                dates.append(dt.strftime('%Y%m%d%H%M'))
            current += timedelta(days=3)

        all_data = []
        for date in dates:
            try:
                df = fetch_typhoon_data(date)
                if not df.empty:
                    # NaN 값을 None으로 변환
                    df = df.replace({pd.NA: None, pd.NaT: None})
                    df = df.where(pd.notnull(df), None)
                    df = process_typhoon_data(df)
                    all_data.append(df)
            except Exception as e:
                logger.error(f"Error processing data for {date}: {str(e)}")
                continue

        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            # 최종 DataFrame의 NaN 값도 처리
            final_df = final_df.replace({pd.NA: None, pd.NaT: None})
            final_df = final_df.where(pd.notnull(final_df), None)
            makefile(final_df, local_file_path)
            bucket_name = Variable.get("de06-8team-thirdproject")
            from plugins import s3_plugins
            s3_plugins.upload_to_s3(
                s3_key,
                bucket_name,
                [local_file_path],
                replace=True,
            )
            context['task_instance'].xcom_push(key='basedate', value=basedate)
        else:
            logger.info(f"No typhoon data available for {basedate}")

    except Exception as e:
        logger.error(f"Error in apidata_to_s3: {str(e)}")
        raise
    finally:
        if local_file_path and os.path.exists(local_file_path):
            try:
                os.remove(local_file_path)
            except Exception as e:
                logger.error(f"Error deleting temporary file: {str(e)}")

def s3_to_snowflake(**context):
    try:
        extra = snow_conn.extra_dejson
        basedate = context['task_instance'].xcom_pull(task_ids='update_daily_typhoon_to_S3', key='basedate')
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

        # 1. Staging 테이블 생성 및 비우기
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_typhoon_stage LIKE raw_typhoon;
            TRUNCATE TABLE raw_typhoon_stage;
        """)

        # 2. S3에서 Staging 테이블로 적재
        copy_sql = f"""
            COPY INTO raw_typhoon_stage
            FROM @my_s3_stage
            FILES = ('typhoon_daily_{basedate}.csv')
            FORCE=TRUE;
        """
        cursor.execute(copy_sql)

        # 3. MERGE INTO 본 테이블 (중복 방지)
        merge_sql = """
            MERGE INTO raw_typhoon AS tgt
            USING raw_typhoon_stage AS src
            ON tgt.TYP_TM = src.TYP_TM AND tgt.TYP = src.TYP
            WHEN MATCHED THEN UPDATE SET
                YY = src.YY, SEQ = src.SEQ, FT = src.FT, LAT = src.LAT, LON = src.LON, DIR = src.DIR,
                SP = src.SP, PS = src.PS, WS = src.WS, RAD15 = src.RAD15, RAD25 = src.RAD25
            WHEN NOT MATCHED THEN
                INSERT (YY, TYP, SEQ, FT, TYP_TM, LAT, LON, DIR, SP, PS, WS, RAD15, RAD25)
                VALUES (src.YY, src.TYP, src.SEQ, src.FT, src.TYP_TM, src.LAT, src.LON, src.DIR, src.SP, src.PS, src.WS, src.RAD15, src.RAD25);
        """
        cursor.execute(merge_sql)

        # 4. Staging 테이블 비우기
        cursor.execute("TRUNCATE TABLE raw_typhoon_stage;")

    except Exception as e:
        logger.error(f"Error in s3_to_snowflake: {str(e)}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# DAG definition
dag = DAG(
    'typhoon_daily_Update_to_Snowflake',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Daily ETL pipeline for typhoon data from KMA API to Snowflake (incremental, deduplication)',
    schedule_interval='0 8 * * *',  # 매일 아침 8시 실행
    start_date=datetime(2025, 8, 20),
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False,
    tags=['typhoon', 'etl'],
)

# Tasks
update_daily_typhoon_to_S3 = PythonOperator(
    task_id='update_daily_typhoon_to_S3',
    python_callable=apidata_to_s3,
    provide_context=True,
    dag=dag,
)

update_daily_S3_to_snowflake = PythonOperator(
    task_id='update_daily_S3_to_snowflake',
    python_callable=s3_to_snowflake,
    provide_context=True,
    dag=dag,
)

# Task dependencies
update_daily_typhoon_to_S3 >> update_daily_S3_to_snowflake
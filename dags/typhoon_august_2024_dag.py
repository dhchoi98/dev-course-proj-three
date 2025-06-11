from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import snowflake.connector
import pandas as pd
from datetime import datetime, timedelta
import logging
import sys
import os

# API 관련 함수는 기존 typhoon_utils에서 import한다고 가정
sys.path.append(os.path.join(os.path.dirname(__file__), "apis"))
from apis.typhoon_utils import fetch_typhoon_data, process_typhoon_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def upsert_to_snowflake(df):
    snow_conn = BaseHook.get_connection('snowflake_conn')
    extra = snow_conn.extra_dejson
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
    try:
        # 1. 임시 테이블 생성
        cursor.execute("CREATE OR REPLACE TEMP TABLE typhoon_stage LIKE raw_typhoon;")
        
        # 2. DataFrame의 timestamp 컬럼을 문자열로 변환하고 NaN 값을 None으로 변환
        if 'TYP_TM' in df.columns:
            df['TYP_TM'] = df['TYP_TM'].astype(str)
        
        # NaN 값을 None으로 변환
        df = df.replace({pd.NA: None, pd.NaT: None})
        df = df.where(pd.notnull(df), None)
        
        # 3. DataFrame을 임시 테이블에 INSERT
        insert_sql = """
            INSERT INTO typhoon_stage (
                YY, TYP, SEQ, FT, TYP_TM, LAT, LON, DIR, SP, PS, WS, RAD15, RAD25
            ) VALUES (
                %s, %s, %s, %s, TO_TIMESTAMP(%s, 'YYYY-MM-DD HH24:MI:SS'), %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        
        for _, row in df.iterrows():
            values = [
                row['YY'] if pd.notnull(row['YY']) else None,
                row['TYP'] if pd.notnull(row['TYP']) else None,
                row['SEQ'] if pd.notnull(row['SEQ']) else None,
                row['FT'] if pd.notnull(row['FT']) else None,
                row['TYP_TM'] if pd.notnull(row['TYP_TM']) else None,
                row['LAT'] if pd.notnull(row['LAT']) else None,
                row['LON'] if pd.notnull(row['LON']) else None,
                row['DIR'] if pd.notnull(row['DIR']) else None,
                row['SP'] if pd.notnull(row['SP']) else None,
                row['PS'] if pd.notnull(row['PS']) else None,
                row['WS'] if pd.notnull(row['WS']) else None,
                row['RAD15'] if pd.notnull(row['RAD15']) else None,
                row['RAD25'] if pd.notnull(row['RAD25']) else None
            ]
            cursor.execute(insert_sql, values)
            
        # 4. MERGE로 중복 방지 upsert
        merge_sql = """
            MERGE INTO raw_typhoon AS tgt
            USING typhoon_stage AS src
            ON tgt.TYP_TM = src.TYP_TM AND tgt.TYP = src.TYP
            WHEN MATCHED THEN UPDATE SET
                YY = src.YY, SEQ = src.SEQ, FT = src.FT, LAT = src.LAT, LON = src.LON, DIR = src.DIR,
                SP = src.SP, PS = src.PS, WS = src.WS, RAD15 = src.RAD15, RAD25 = src.RAD25
            WHEN NOT MATCHED THEN
                INSERT (YY, TYP, SEQ, FT, TYP_TM, LAT, LON, DIR, SP, PS, WS, RAD15, RAD25)
                VALUES (src.YY, src.TYP, src.SEQ, src.FT, src.TYP_TM, src.LAT, src.LON, src.DIR, src.SP, src.PS, src.WS, src.RAD15, src.RAD25);
        """
        cursor.execute(merge_sql)
        logger.info("MERGE INTO raw_typhoon completed.")
    finally:
        cursor.close()
        conn.close()

def etl_task(**context):
    # 고정된 날짜 범위 설정
    start_date = datetime(2024, 8, 10)
    end_date = datetime(2024, 8, 14)
    
    # 6시간 간격으로 날짜 생성
    dates = []
    current = start_date
    while current <= end_date:
        for hour in [0, 6, 12, 18]:
            dt = current.replace(hour=hour, minute=0)
            dates.append(dt.strftime('%Y%m%d%H%M'))
        current += timedelta(days=1)
    
    logger.info(f"Processing dates from {start_date} to {end_date}")
    all_data = []
    
    for date in dates:
        try:
            df = fetch_typhoon_data(date)
            if not df.empty:
                df = process_typhoon_data(df)
                all_data.append(df)
                logger.info(f"Successfully processed data for {date}")
        except Exception as e:
            logger.error(f"Error processing data for {date}: {str(e)}")
            continue
    
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        upsert_to_snowflake(final_df)
        logger.info(f"ETL and upsert to Snowflake completed for period {start_date} to {end_date}")
    else:
        logger.warning(f"No typhoon data found for period {start_date} to {end_date}")

dag = DAG(
    dag_id='typhoon_august_2024_dag',
    start_date=datetime(2024, 1, 1),  # 과거 날짜로 설정
    schedule_interval=None,  # 수동 실행만 가능하도록 설정
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Load typhoon data for August 10-14, 2024 period',
    tags=['typhoon', 'etl', 'august_2024'],
)

etl = PythonOperator(
    task_id='typhoon_etl',
    python_callable=etl_task,
    provide_context=True,
    dag=dag,
) 
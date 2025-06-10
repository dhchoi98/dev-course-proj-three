from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import os

API_URL = "https://apihub.kma.go.kr/api/typ01/url/dst_pm10_hr.php"
API_KEY = "1YHNOyKjQZKBzTsio9GS3g"
ORG = "'kma'"
S3_CONN_ID = "my_s3_conn"
S3_BUCKET = "de06-8team-thirdproject"
S3_KEY = "dust/monthly_avg_by_station.csv"
SNOWFLAKE_CONN_ID = "my_snowflake_conn"
SNOWFLAKE_STAGE = "@DEVCOURSE.RAW_DATA.MY_S3_STAGE2" 
SNOWFLAKE_TABLE = "RAW_DATA.MONTHLY_DUST_AVG_BY_STATION"

# 하루 단위로 API 호출 (tm=YYYYMMDD0000, tm_st=YYYYMMDD0000)
def fetch_avg_by_day(start_date, end_date):
    results = []
    current = start_date
    last_month = None
    while current <= end_date:
        tm = current.strftime('%Y%m%d0000')
        tm_st = current.strftime('%Y%m%d0000')
        params = {
            'tm': tm,
            'tm_st': tm_st,
            'help': '1',
            'org': ORG,
            'authKey': API_KEY
        }
        response = requests.get(API_URL, params=params)
        response.encoding = 'euc-kr'
        lines = response.text.splitlines()
        for line in lines:
            if line.startswith('#') or line.startswith('-') or not line.strip():
                continue
            parts = line.split()
            if len(parts) < 6:
                continue
            try:
                kst = parts[0]
                stn_id = parts[2]
                avg_cnt = parts[3].split('(')[0]  # 25(12) → 25
                avg_cnt = float(avg_cnt)
                results.append({
                    'kst': kst,
                    'STN_ID': stn_id,
                    'avg': avg_cnt
                })
            except Exception:
                continue
        # 월별 완료 메시지 출력
        current_month = current.strftime('%Y-%m')
        if last_month is not None and last_month != current_month:
            print(f"{last_month} 데이터 수집 완료")
        last_month = current_month
        current += timedelta(days=1)
    # 마지막 월도 출력
    if last_month is not None:
        print(f"{last_month} 데이터 수집 완료")
    return pd.DataFrame(results)

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="monthly_dust_avg_to_csv",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # 수동 실행 권장
    catchup=False,
    default_args=default_args,
    description="2024~2025년 미세먼지 STN_ID별 월별 평균 집계, S3 업로드, Snowflake COPY INTO"
) as dag:

    @task()
    def get_and_save():
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 12, 31)
        df = fetch_avg_by_day(start_date, end_date)
        if df.empty:
            print("No data fetched!")
            return None
        df['kst'] = pd.to_datetime(df['kst'], format='%Y.%m.%d.%H:%M')
        df['month'] = df['kst'].dt.to_period('M')
        df_monthly = df.groupby(['STN_ID', 'month'])['avg'].mean().reset_index(name='monthly_avg')
        local_path = '/tmp/monthly_avg_by_station.csv'
        df_monthly.to_csv(local_path, index=False)
        print(f"Saved {local_path}")
        return local_path

    @task()
    def upload_to_s3(local_path: str):
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        hook.load_file(
            filename=local_path,
            key=S3_KEY,
            bucket_name=S3_BUCKET,
            replace=True
        )
        print(f"Uploaded to s3://{S3_BUCKET}/{S3_KEY}")
        # 임시 파일 삭제
        try:
            os.remove(local_path)
            print(f"Deleted local file: {local_path}")
        except Exception as e:
            print(f"Failed to delete local file: {e}")
        return f"s3://{S3_BUCKET}/{S3_KEY}"

    copy_sql = f'''
    USE SCHEMA RAW_DATA;
    CREATE TABLE IF NOT EXISTS MONTHLY_DUST_AVG_BY_STATION (
        STN_ID STRING,
        MONTH STRING,
        MONTHLY_AVG FLOAT
    );
    TRUNCATE TABLE MONTHLY_DUST_AVG_BY_STATION;
    COPY INTO MONTHLY_DUST_AVG_BY_STATION
    FROM @DEVCOURSE.RAW_DATA.MY_S3_STAGE2/monthly_avg_by_station.csv
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
    FORCE=TRUE;
    '''

    copy_into_snowflake = SnowflakeOperator(
        task_id="copy_into_snowflake",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=copy_sql
    )

    # DAG Task Dependency
    s3_path = upload_to_s3(get_and_save())
    s3_path >> copy_into_snowflake 
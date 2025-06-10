from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests

SNOWFLAKE_CONN_ID = "my_snowflake_conn"
RAW_TABLE = "RAW_DATA.HOURLY_DUST"
API_URL = "https://apihub.kma.go.kr/api/typ01/url/dst_pm10_tm.php"
API_PARAMS = {
    "help": "1",
    "org": "'kma'",
    "authKey": "zxiqblJaSgSYqm5SWioEeA"
}

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="hourly_dust_to_snowflake",
    start_date=datetime(2024, 6, 8, 0, 0),
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args,
    description="매시간 미세먼지 API → Snowflake 직접 적재 DAG (TaskFlow API, 중복 제거)"
) as dag:

    @task()
    def fetch_dust_data(execution_date=None):
        dt = execution_date or datetime.now()
        tm = dt.strftime("%Y%m%d%H00")
        params = API_PARAMS.copy()
        params["tm"] = tm

        response = requests.get(API_URL, params=params)
        response.encoding = "euc-kr"
        lines = response.text.splitlines()

        columns = ["DATE", "ORG", "STN", "PM10"]
        data_rows = []
        for line in lines:
            if line.startswith("#") or not line.strip():
                continue
            values = line.split()
            if len(values) < len(columns):
                values += [''] * (len(columns) - len(values))
            data_rows.append(tuple(values[:len(columns)]))
        # (DATE, STN)별로 중복 제거
        deduped = {}
        for row in data_rows:
            key = (row[0], row[2])
            if key not in deduped:
                deduped[key] = row
        return list(deduped.values())

    @task()
    def insert_to_snowflake(rows):
        if not rows:
            print("No data to insert.")
            return
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        # 1. 테이블이 없으면 생성
        create_sql = f'''
        CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
            DATE STRING,
            ORG STRING,
            STN STRING,
            PM10 STRING
        );
        '''
        hook.run(create_sql)
        # 2. 데이터 insert
        hook.insert_rows(
            table=RAW_TABLE,
            rows=rows,
            target_fields=["DATE", "ORG", "STN", "PM10"]
        )

    insert_to_snowflake(fetch_dust_data()) 
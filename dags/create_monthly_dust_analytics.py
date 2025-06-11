from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

SNOWFLAKE_CONN_ID = "my_snowflake_conn"
ANALYTICS_TABLE = "ANALYTICS.MONTHLY_DUST_ANALYTICS"

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="create_monthly_dust_analytics",
    start_date=datetime(2024, 6, 8, 0, 0),
    schedule_interval='@monthly',
    catchup=False,
    default_args=default_args,
    description="월별 미세먼지 평균 + 지점정보(이름만) 조인 분석 테이블 생성 (MONTH를 DATE 포맷으로 변환)"
) as dag:

    create_or_replace = SnowflakeOperator(
        task_id="create_or_replace_monthly_analytics_table",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        CREATE OR REPLACE TABLE {ANALYTICS_TABLE} AS
        SELECT
            m.STN_ID,
            sm.STN_KO,
            TO_DATE(m.MONTH || '-01') AS MONTH,
            m.MONTHLY_AVG
        FROM RAW_DATA.MONTHLY_DUST_AVG_BY_STATION m
        JOIN RAW_DATA.STATION_MASTER sm
            ON m.STN_ID = sm.STN_ID;
        """
    ) 
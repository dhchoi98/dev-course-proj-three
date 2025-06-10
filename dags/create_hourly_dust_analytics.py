from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

SNOWFLAKE_CONN_ID = "my_snowflake_conn"
ANALYTICS_TABLE = "ANALYTICS.HOURLY_DUST_ANALYTICS"

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="create_hourly_dust_analytics",
    start_date=datetime(2024, 6, 8, 0, 0),
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args,
    description="최신 관측값만 유지하며 중복 없는 ANALYTICS.HOURLY_DUST_ANALYTICS (ROW_NUMBER deduplication, PM10 등급 추가, PM10 숫자형 변환)"
) as dag:

    create_or_replace = SnowflakeOperator(
        task_id="create_or_replace_analytics_table",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        CREATE OR REPLACE TABLE {ANALYTICS_TABLE} AS
        WITH deduped AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY DATE, STN ORDER BY DATE DESC) AS rn
            FROM RAW_DATA.HOURLY_DUST
            WHERE DATE = (SELECT MAX(DATE) FROM RAW_DATA.HOURLY_DUST)
        )
        SELECT
            d.DATE,
            sm.STN_ID,
            sm.STN_KO,
            sm.LON,
            sm.LAT,
            TRY_CAST(d.PM10 AS NUMBER) AS PM10,
            CASE
                WHEN TRY_CAST(d.PM10 AS FLOAT) <= 30 THEN '좋음'
                WHEN TRY_CAST(d.PM10 AS FLOAT) <= 80 THEN '보통'
                WHEN TRY_CAST(d.PM10 AS FLOAT) <= 150 THEN '나쁨'
                WHEN TRY_CAST(d.PM10 AS FLOAT) > 150 THEN '매우나쁨'
                ELSE '정보없음'
            END AS DUST_STATUS
        FROM deduped d
        JOIN RAW_DATA.STATION_MASTER sm
            ON d.STN = sm.STN_ID
        WHERE d.rn = 1;
        """
    ) 
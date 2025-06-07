from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from scripts.weather_utils import format_simple_datetime

def insert_weather_score(**kwargs):
    hook = SnowflakeHook(snowflake_conn_id="my_snowflake_conn")

    # ✅ 실행 날짜 포맷팅
    execution_date = kwargs["execution_date"]  # datetime 형식

    insert_sql = f"""
        INSERT INTO WEATHER_DB.JINYOUNG.WEATHER_SCORE (
            region_code, region_name, date, avg_temp, wind_speed, precipitation, score
        )
        VALUES (
            '{kwargs["region_code"]}', '{kwargs["region_name"]}', '{execution_date}',
            {kwargs["avg_temp"]}, {kwargs["wind_speed"]},
            {kwargs["precipitation"]}, {kwargs["score"]}
        )
    """

    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(insert_sql)
        conn.commit()
    finally:
        cursor.close()
        conn.close()

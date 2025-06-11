from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import parse

def insert_weather_score(**kwargs):
    hook = SnowflakeHook(snowflake_conn_id="my_snowflake_conn")

    # datetime 형식
    execution_date = kwargs["execution_date"]
    if isinstance(execution_date, str):
        execution_date = parse(execution_date)

    date_str = execution_date.strftime("%Y-%m-%d")

    merge_sql = f"""
    MERGE INTO WEATHER_DB.JINYOUNG.WEATHER_SCORE tgt
    USING (
        SELECT
            '{kwargs["region_code"]}'   AS region_code,
            '{kwargs["region_name"]}'   AS region_name,
            '{date_str}'          AS date,
            {kwargs["avg_temp"]}        AS avg_temp,
            {kwargs["wind_speed"]}      AS wind_speed,
            {kwargs["precipitation"]}   AS precipitation,
            {kwargs["humidity"]}        AS humidity,
            {kwargs["ground_temp"]}     AS ground_temp,
            {kwargs["wind_dir"]}        AS wind_dir,
            {kwargs["score"]}           AS score
    ) src
    ON tgt.region_code = src.region_code
        AND tgt.date        = src.date
    WHEN MATCHED THEN
        UPDATE SET
        region_name  = src.region_name,
        avg_temp     = src.avg_temp,
        wind_speed   = src.wind_speed,
        precipitation= src.precipitation,
        humidity     = src.humidity,
        ground_temp  = src.ground_temp,
        wind_dir     = src.wind_dir,
        score        = src.score
    WHEN NOT MATCHED THEN
        INSERT (region_code, region_name, date, avg_temp, wind_speed, precipitation, humidity, ground_temp, wind_dir, score)
        VALUES (src.region_code, src.region_name, src.date, src.avg_temp, src.wind_speed, 
        src.precipitation, src.humidity, src.ground_temp, src.wind_dir, src.score)
    ;
    """

    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(merge_sql)
        conn.commit()
    finally:
        cursor.close()
        conn.close()

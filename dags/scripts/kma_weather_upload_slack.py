# 설명: 처리된 날씨 데이터를 외부 서비스에 저장하거나 알림을 보내는 기능을 담당합니다.
# S3에 CSV 파일을 업로드하고, Snowflake에 데이터를 적재하며, Slack으로 성공/실패 알림을 전송합니다.

import boto3
import pandas as pd
from io import StringIO
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from botocore.exceptions import NoCredentialsError
import requests
from weather_config import logger, S3_BUCKET, S3_PATH_PREFIX, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_TABLE, SLACK_WEBHOOK_URL, BASE_DATE

def upload_to_s3(df):
    if df.empty:
        logger.warning("S3에 업로드할 데이터가 없습니다.")
        return

    s3_path = f"{S3_PATH_PREFIX}wind_forecast_korea_{BASE_DATE}.csv"
    latest_s3_path = "wind_forecast/latest_forecast.csv"

    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_DEFAULT_REGION
        )
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        csv_data = csv_buffer.getvalue()

        s3.put_object(Bucket=S3_BUCKET, Key=s3_path, Body=csv_data)
        logger.info(f"S3 업로드 완료: s3://{S3_BUCKET}/{s3_path}")

        s3.put_object(Bucket=S3_BUCKET, Key=latest_s3_path, Body=csv_data)
        logger.info(f"S3 최신 파일 업로드 완료: s3://{S3_BUCKET}/{latest_s3_path}")
    except NoCredentialsError:
        logger.error("AWS 자격 증명을 찾을 수 없습니다.")
        raise
    except Exception as e:
        logger.error(f"S3 업로드 오류: {str(e)}")
        raise

def upload_to_snowflake(df):
    if df.empty:
        logger.warning("Snowflake에 업로드할 데이터가 없습니다.")
        return

    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        
        cursor = conn.cursor()
        
        cursor.execute(f"""
            DELETE FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
            WHERE FCST_DATE < '{BASE_DATE}'
        """)
        logger.info(f"Deleted data from {SNOWFLAKE_TABLE} where FCST_DATE < {BASE_DATE}")

        df.columns = [col.upper() for col in df.columns]
        
        temp_table = f"{SNOWFLAKE_TABLE}_TEMP"
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=temp_table,
            auto_create_table=True,
            overwrite=True
        )
        
        cursor.execute(f"SHOW TABLES LIKE '{SNOWFLAKE_TABLE}' IN SCHEMA {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
        tables = cursor.fetchall()
        if not tables:
            create_table_query = """
                CREATE TABLE {}.{}.{} (
                    BASE_DATE VARCHAR,
                    BASE_TIME VARCHAR,
                    FCST_DATE VARCHAR,
                    FCST_TIME VARCHAR,
                    NX INTEGER,
                    NY INTEGER,
                    LOCATION VARCHAR,
                    BROAD_LOCATION VARCHAR,
                    WSD FLOAT,
                    VEC FLOAT,
                    TMP FLOAT,
                    POP FLOAT,
                    REH FLOAT,
                    SKY VARCHAR,
                    PTY VARCHAR,
                    STRONG_WIND_ALERT VARCHAR,
                    DI FLOAT,
                    DISCOMFORT_ALERT VARCHAR
                )
            """.format(SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_TABLE)
            cursor.execute(create_table_query)
            logger.info(f"테이블 {SNOWFLAKE_TABLE} 생성 완료")

        cursor.execute(f"""
            MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} AS target
            USING {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{temp_table} AS source
            ON target.BASE_DATE = source.BASE_DATE
            AND target.FCST_DATE = source.FCST_DATE
            AND target.FCST_TIME = source.FCST_TIME
            AND target.LOCATION = source.LOCATION
            WHEN MATCHED THEN UPDATE SET
                BASE_TIME = source.BASE_TIME,
                NX = source.NX,
                NY = source.NY,
                BROAD_LOCATION = source.BROAD_LOCATION,
                WSD = source.WSD,
                VEC = source.VEC,
                TMP = source.TMP,
                POP = source.POP,
                REH = source.REH,
                SKY = source.SKY,
                PTY = source.PTY,
                STRONG_WIND_ALERT = source.STRONG_WIND_ALERT,
                DI = source.DI,
                DISCOMFORT_ALERT = source.DISCOMFORT_ALERT
            WHEN NOT MATCHED THEN INSERT (
                BASE_DATE, BASE_TIME, FCST_DATE, FCST_TIME, NX, NY,
                LOCATION, BROAD_LOCATION, WSD, VEC, TMP, POP, REH,
                SKY, PTY, STRONG_WIND_ALERT, DI, DISCOMFORT_ALERT
            ) VALUES (
                source.BASE_DATE, source.BASE_TIME, source.FCST_DATE, source.FCST_TIME, source.NX, source.NY,
                source.LOCATION, source.BROAD_LOCATION, source.WSD, source.VEC, source.TMP, source.POP, source.REH,
                source.SKY, source.PTY, source.STRONG_WIND_ALERT, source.DI, source.DISCOMFORT_ALERT
            )
        """)
        
        cursor.execute(f"DROP TABLE IF EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{temp_table}")
        
        logger.info(f"Snowflake 업로드 완료: {nrows} rows processed into {SNOWFLAKE_TABLE}")
        conn.close()
    except Exception as e:
        logger.error(f"Snowflake 업로드 오류: {str(e)}")
        raise

def send_slack_message(message):
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json={"text": message})
        response.raise_for_status()
        logger.info("Slack 메시지 전송 완료")
    except Exception as e:
        logger.error(f"Slack 전송 오류: {str(e)}")
        raise
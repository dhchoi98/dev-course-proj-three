# 설명: 날씨 예보 ETL 파이프라인의 설정을 관리합니다.
# .env 파일에서 환경 변수를 로드하고, 로깅을 설정하며, API, AWS S3, Snowflake, Slack 관련 상수를 정의합니다.
# 다른 모듈에서 이 설정을 import하여 사용합니다.

import os
import logging
from datetime import datetime
import pytz
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler('etl.log', encoding='utf-8')]
)
logger = logging.getLogger(__name__)

# 날씨 API 설정
API_URL = os.getenv("API_URL")
AUTH_KEY = os.getenv("AUTH_KEY")
BASE_DATE = datetime.now(pytz.timezone('Asia/Seoul')).strftime("%Y%m%d")
BASE_TIME = os.getenv("BASE_TIME")

# AWS S3 설정
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PATH_PREFIX = f"wind_forecast/{BASE_DATE}/"
S3_COORDINATES_PATH = os.getenv("S3_COORDINATES_PATH")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")

# Slack Webhook URL
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# Snowflake 설정
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE")
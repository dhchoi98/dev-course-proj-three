from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import logging
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

# --- 로깅 설정 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler('airflow_dag.log', encoding='utf-8')]
)
logger = logging.getLogger(__name__)

# scripts 폴더 경로 추가
scripts_path = os.path.join(os.path.dirname(__file__), '..', 'scripts')
scripts_path = os.path.abspath(scripts_path)
if scripts_path not in sys.path:
    sys.path.insert(0, scripts_path)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_weather_pipeline():
    try:
        # AWS 자격 증명 환경 변수 확인 (ETL에서 S3 업로드에 필요)
        if not os.getenv('AWS_ACCESS_KEY_ID') or not os.getenv('AWS_SECRET_ACCESS_KEY'):
            logger.warning("AWS 자격 증명이 환경 변수에 설정되지 않았습니다. ~/.aws/credentials 또는 .env 파일을 확인하세요.")
        
        # kma_weather_etl 모듈 임포트 및 실행
        from kma_weather_etl import main
        main()
        logger.info("Weather pipeline executed successfully")
    except Exception as e:
        logger.error(f"Weather pipeline failed: {str(e)}")
        raise

with DAG(
    'kma_weather_forecast_dag',
    default_args=default_args,
    schedule_interval='0 2,5,8,11,14,17,20,23 * * *',  
    catchup=False,
    tags=['kma', 'weather']
) as dag:

    task_run_pipeline = PythonOperator(
        task_id='run_weather_pipeline',
        python_callable=run_weather_pipeline,
    )

    task_run_pipeline
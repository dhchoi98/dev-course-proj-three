from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import requests
import csv
import os

# dbt 프로젝트 경로 설정
DBT_PROJECT_DIR = os.getenv('DBT_PROJECT_DIR', '/opt/airflow/dbts')

def fetch_and_filter_station_info():
    API_URL = "https://apihub.kma.go.kr/api/typ01/url/stn_pm10_inf.php"
    PARAMS = {
        "inf": "'kma'",
        "tm": "201012310900",
        "help": "1",
        "org": "'kma'",
        "authKey": "1YHNOyKjQZKBzTsio9GS3g"
    }
    # dbt seed 폴더에 저장
    SAVE_PATH = os.path.join(DBT_PROJECT_DIR, "seeds", "station_master.csv")
    os.makedirs(os.path.dirname(SAVE_PATH), exist_ok=True)

    columns = [
        "STN_ID", "TM_ED", "TM_ST", "STN_KO", "STN_EN", "STN_SP",
        "LON", "LAT", "HT", "STN_AD", "FCT_ID"
    ]

    response = requests.get(API_URL, params=PARAMS)
    try:
        response.encoding = 'euc-kr'
        lines = response.text.splitlines()
    except:
        response.encoding = 'utf-8'
        lines = response.text.splitlines()

    data_started = False
    data_rows = []

    for line in lines:
        if '--------------------------' in line:
            data_started = True
            continue
        if not data_started or line.startswith('#') or not line.strip():
            continue
        values = line.split()
        if len(values) < len(columns):
            values += [''] * (len(columns) - len(values))
        data_rows.append(values[:len(columns)])

    KEEP_IDS = {"90", "100", "101", "102", "108", "115", "119", "121", "130", "135", "136", "140", "143", "146", "152", "156", "169", "175", "185", "192", "201", "232"}
    filtered_rows = [row for row in data_rows if row[0].strip() in KEEP_IDS]

    with open(SAVE_PATH, mode="w", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)
        writer.writerows(filtered_rows)


with DAG(
    dag_id="station_master_seed_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
    catchup=False,
    description="Download, filter, and seed station master table to Snowflake"
) as dag:

    fetch_and_filter = PythonOperator(
        task_id="fetch_and_filter_station_info",
        python_callable=fetch_and_filter_station_info
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed_station_master",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt seed --select station_master",
        env={
            "DBT_PROJECT_DIR": DBT_PROJECT_DIR,
            "PATH": os.environ.get("PATH", ""),
            "PYTHONPATH": os.environ.get("PYTHONPATH", "")
        }
    )

    fetch_and_filter >> dbt_seed 
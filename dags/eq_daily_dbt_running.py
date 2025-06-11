from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

import os
from datetime import datetime
from datetime import timedelta

# Airflow 컨테이너 내부에서 볼 수 있는 경로로 수정
project_path = '/usr/app'  # Airflow 컨테이너에 마운트된 경로
profile_path = '/root/.dbt/profiles.yml'  # Airflow 컨테이너에 마운트된 경로

print('project_path : ' + project_path)
print('profile_path : ' + profile_path)

dag = DAG(
    dag_id = 'eq_daily_dbt_running',
    start_date = datetime(2025,6,4), 
    schedule = '0 9 * * *',  # 적재작업 8시 dbt run은 9시
    max_active_runs = 1,
    max_active_tasks = 1,
    catchup = False
)


run_daily_dbt = DockerOperator(
    task_id='run_daily_dbt',
    image='ghcr.io/dbt-labs/dbt-snowflake:1.9.latest',
    api_version='auto',
    auto_remove=True,
    command='dbt run',
    docker_url='unix://var/run/docker.sock',  # 로컬 Docker 엔진을 사용할 경우
    network_mode='bridge',
    working_dir='/usr/app',
    mounts=[
        # 이 부분이 핵심!
        {
            "Source": "/usr/app",
            "Target": "/usr/app",
            "Type": "bind"
        },
        {
            "Source": "/root/.dbt",
            "Target": "/root/.dbt",
            "Type": "bind"
        }
    ],
    dag=dag
)

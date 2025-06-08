import sys
import os

# scripts 디렉토리를 모듈 경로로 추가
sys.path.append(os.path.join(os.path.dirname(__file__), "../scripts"))

from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

from weather_extract import extract_weather, region_name_to_code
from weather_utils import (
    summarize_weather,
    calculate_outdoor_score,
    is_weekend,
    is_holiday
)
from weather_load_score import insert_weather_score
from slack_alert import slack_on_failure_callback  

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": slack_on_failure_callback
}

with DAG(
    dag_id="weather_pipeline_dag",
    # start_date=datetime(2025, 1, 6),  # 월요일 시작
    start_date=datetime(2025, 6, 2),  # 월요일 시작
    schedule_interval="@weekly",      # 매주 1회 실행
    catchup=True,
    default_args=default_args,
    tags=["weather", "etl"],
) as dag:

    @task
    def extract(execution_date=None):
        region_name = "서울"
        region_code = region_name_to_code[region_name]

        # ✅ 주간 범위 계산: 일주일 전 월요일 ~ 전날 일요일
        end_date = execution_date - timedelta(days=1)
        start_date = end_date - timedelta(days=6)

        tm1 = start_date.strftime("%Y%m%d") + "0000"
        tm2 = end_date.strftime("%Y%m%d") + "2300"

        print(f" 날씨 수집 기간: {tm1} ~ {tm2}")

        records = extract_weather(region_code, tm1, tm2)

        return {
            "region_name": region_name,
            "region_code": region_code,
            "execution_date": execution_date.strftime("%Y-%m-%d %H:%M"),
            "records": records
        }

    @task
    def transform(extracted):
        records = extracted["records"]

        #  주말 또는 공휴일만 필터링
        filtered = [
            r for r in records
            if is_weekend(r["timestamp_readable"]) or is_holiday(r["timestamp_readable"])
        ]

        if not filtered:
            print(" 주말/공휴일 데이터 없음, 적재 스킵")
            return None

        summary = summarize_weather(filtered)
        score = calculate_outdoor_score(summary)

        return {
            **extracted,
            "summary": summary,
            "score": score
        }

    @task
    def load(transformed):
        if not transformed:
            print("🚫 적재할 데이터 없음")
            return

        insert_weather_score(
            region_code=transformed["region_code"],
            region_name=transformed["region_name"],
            avg_temp=transformed["summary"]["avg_temp"],
            wind_speed=transformed["summary"]["wind_speed"],
            precipitation=transformed["summary"]["precipitation"],
            score=transformed["score"],
            execution_date=transformed["execution_date"]
        )

        print(f"✅ {transformed['region_name']} 점수 적재 완료: {transformed['score']}점")

        return transformed

    @task
    def notify(transformed):
        if not transformed:
            print(" 알림 생략: 데이터 없음")
            return

        from weather_utils import (
            describe_days,
            format_slack_message,
            send_slack_message
        )

        period = transformed["execution_date"][:10]
        summary = transformed["summary"]
        score = transformed["score"]
        days = describe_days(transformed["records"])

        message = format_slack_message(
            transformed["region_name"], period, score, summary, days
        )

        print("메시지 전송 내용:")
        print(message)

        send_slack_message(message) 

    # DAG 연결 (execution_date는 Airflow가 자동 주입함)
    notify(load(transform(extract())))

import sys
import os
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

# scripts 디렉토리를 모듈 경로로 추가
sys.path.append(os.path.join(os.path.dirname(__file__), "../scripts"))

from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from typing import List, Dict, Any

from weather_extract import extract_weather, region_name_to_code
from weather_utils import (
    summarize_weather,
    calculate_outdoor_score,
    is_weekend,
    is_holiday,
    describe_days,
    format_slack_message,
    send_slack_message
)
from weather_load_score import insert_weather_score
from slack_alert import slack_on_failure_callback  

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": slack_on_failure_callback
}

regions = list(region_name_to_code.keys())

with DAG(
    dag_id="weather_pipeline_dag",
    start_date=datetime(2025, 1, 6),  # 월요일 시작
    schedule_interval="@weekly",      # 매주 1회 실행
    catchup=True,
    default_args=default_args,
    tags=["weather", "etl"],
) as dag:

    @task
    def extract(region_name: str):
        context = get_current_context()
        execution_date = context["execution_date"]

        # ✅ 주간 범위 계산: 일주일 전 월요일 ~ 전날 일요일
        region_code = region_name_to_code[region_name]
        end_date = execution_date - timedelta(days=1)
        start_date = end_date - timedelta(days=6)

        tm1 = start_date.strftime("%Y%m%d") + "0000"
        tm2 = end_date.strftime("%Y%m%d") + "2300"
        print(f" 날씨 수집 기간: {tm1} ~ {tm2}")

        records = extract_weather(region_code, tm1, tm2)

        return {
            "region_name": region_name,
            "region_code": region_code,
            "execution_date": execution_date,
            "period_start": tm1,
            "period_end": tm2,
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
            "records": filtered,
            "summary": summary,
            "score": score,
        }


    @task
    def load(transformed):
        if not transformed:
            print(" 주말/공휴일 데이터 없음, 적재 스킵")
            return None

        insert_weather_score(
            region_code=transformed["region_code"],
            region_name=transformed["region_name"],
            avg_temp=transformed["summary"]["avg_temp"],
            wind_speed=transformed["summary"]["wind_speed"],
            precipitation=transformed["summary"]["precipitation"],
            humidity=transformed["summary"]["humidity"],        
            ground_temp=transformed["summary"]["ground_temp"],   
            wind_dir=transformed["summary"]["wind_dir"],       
            score=transformed["score"],
            execution_date=transformed["execution_date"]
        )

        print(f"✅ {transformed['region_name']} 점수 적재 완료: {transformed['score']}점")

        return transformed

    @task
    def notify(transformed_list: List[Dict[str, Any]]):
        if not transformed_list:
            print(" 알림 생략: 데이터 없음")
            return

        # None 값 제거 및 점수 기준 정렬
        valid_data = [t for t in transformed_list if t is not None]
        if not valid_data:
            print(" 알림 생략: 유효한 데이터 없음")
            return

        # 점수 기준으로 정렬
        sorted_data = sorted(valid_data, key=lambda x: x["score"], reverse=True)
        
        # 상위 3개, 하위 3개 지역 선택
        top_n = 3
        top_regions = sorted_data[:top_n]
        bottom_regions = sorted_data[-top_n:]

        # YYYYMMDDHHMM → YYYY-MM-DD 로 포맷 간단 변환
        start = valid_data[0]["period_start"]
        end = valid_data[0]["period_end"]
        period = f"{start[:4]}-{start[4:6]}-{start[6:8]} ~ {end[:4]}-{end[4:6]}-{end[6:8]}"

        # 전체 메시지 시작
        full_message = f"""📊 주간 날씨 만족도 분석 결과 ({period})

{'='*30}
🏆 상위 지역
{'='*30}
"""
        # 상위 지역 메시지 생성
        for i, region in enumerate(top_regions, 1):
            days = describe_days(region["records"])
            full_message += f"\n{i}위: {region['region_name']}\n"
            full_message += format_slack_message(
                region["region_name"], period,
                region["score"], region["summary"], days
            )
            if i < len(top_regions):
                full_message += "\n" + "-"*30 + "\n"

        # 구분선 추가
        full_message += f"""

{'='*30}
📉 하위 지역
{'='*30}
"""
        # 하위 지역 메시지 생성
        for i, region in enumerate(bottom_regions, 1):
            days = describe_days(region["records"])
            full_message += f"\n{i}위: {region['region_name']}\n"
            full_message += format_slack_message(
                region["region_name"], period,
                region["score"], region["summary"], days
            )
            if i < len(bottom_regions):
                full_message += "\n" + "-"*30 + "\n"
        
        print("메시지 전송:\n" + full_message)
        send_slack_message(full_message)

    # notify(load(transform(extract())))
    # Dynamic Task Mapping for individual regions
    extracted_list = extract.expand(region_name=regions)
    transformed_list = transform.expand(extracted=extracted_list)
    loaded_list = load.expand(transformed=transformed_list)
    
    # 모든 지역의 데이터를 수집한 후 한 번에 알림 전송
    notify.override(trigger_rule=TriggerRule.ALL_SUCCESS)(loaded_list)

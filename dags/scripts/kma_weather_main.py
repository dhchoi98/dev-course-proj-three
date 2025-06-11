# 설명: 날씨 예보 ETL 파이프라인의 전체 흐름을 조율합니다.
# 데이터 수집, 처리, 저장, 알림 단계를 순차적으로 실행하며, 에러 처리 및 Slack 알림을 포함합니다.

from weather_data import fetch_data, process_data
from weather_services import upload_to_s3, upload_to_snowflake, send_slack_message
from weather_config import logger

def main():
    try:
        raw_df = fetch_data()
        if raw_df.empty:
            logger.warning("수집된 데이터가 없습니다.")
            send_slack_message(":warning: 날씨 데이터 수집 실패: 데이터 없음")
            return

        processed_df = process_data(raw_df)
        if processed_df.empty:
            logger.warning("처리된 데이터가 없습니다.")
            send_slack_message(":warning: 날씨 데이터 처리 실패: 데이터 없음")
            return

        upload_to_s3(processed_df)
        upload_to_snowflake(processed_df)

        logger.info("날씨 데이터 수집 및 적재 완료!")
        send_slack_message("*✅ 날씨 데이터 수집 및 적재 완료!*")
        send_slack_message("\n*🌍 날씨 정보를 알고 싶다면 저를 불러주세요! ✨*")
    except Exception as e:
        logger.error(f"ETL 파이프라인 오류: {str(e)}")
        send_slack_message(f":warning: 날씨 데이터 처리 중 오류 발생: {str(e)}")
        raise

if __name__ == "__main__":
    main()
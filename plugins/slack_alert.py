import requests
from airflow.models import Variable

def slack_on_failure_callback(context):
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url

    message = f"""
❌ DAG 실패 알림 ❌

• DAG ID: `{dag_id}`
• Task ID: `{task_id}`
• 실행 시각: `{execution_date}`
• 로그 보기: {log_url}
"""

    webhook_url = Variable.get("slack_webhook_url")
    payload = {"text": message}

    try:
        response = requests.post(webhook_url, json=payload)
        if response.status_code == 200:
            print(" Slack 실패 알림 전송 성공")
        else:
            print(f" Slack 전송 실패: {response.status_code}")
    except Exception as e:
        print(f"⚠️ Slack 전송 중 예외 발생: {e}")

from airflow.models import Variable
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def send_slack_notification(context, message, is_error=False):
    """Slack 알림을 보내는 함수"""
    slack_webhook_token = Variable.get("slack_webhook_token")           #슬랙 웹훅 만들어서 추가
    emoji = ":x:" if is_error else ":white_check_mark:"
    
    slack_msg = f"""
    {emoji} *Task Failed* {emoji}
    *DAG*: {context['dag'].dag_id}
    *Task*: {context['task'].task_id}
    *Execution Time*: {context['execution_date']}
    *Message*: {message}
    """
    
    slack_operator = SlackWebhookOperator(
        task_id='slack_notification',
        webhook_conn_id='slack_webhook',
        message=slack_msg,
        channel='#airflow-alerts'
    )
    slack_operator.execute(context)
    return ''
FROM apache/airflow:2.9.1-python3.11

USER airflow
RUN pip install --no-cache-dir dbt-snowflake \
    'protobuf<5.0.0' \
    'proto-plus<2.0.0' \
    'proto<5.0.0' \
    'google-cloud-core<2.4.0'

USER root 
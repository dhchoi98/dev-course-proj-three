FROM apache/airflow:2.9.1

USER airflow
RUN pip install holidays
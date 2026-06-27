FROM datn-python-base:latest

WORKDIR /opt/airflow
ENV AIRFLOW_HOME=/opt/airflow

CMD ["airflow", "scheduler"]
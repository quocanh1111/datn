FROM datn-python-base:latest

WORKDIR /opt/airflow
ENV AIRFLOW_HOME=/opt/airflow

EXPOSE 8080

CMD ["airflow", "webserver", "-p", "8080"]
FROM apache/airflow:2.10.4-python3.10

# Set the working directory (optional, just for organizational purposes)
WORKDIR /data_project/airflow-schedular

# Run the scheduler
CMD [ "airflow", "scheduler" ]
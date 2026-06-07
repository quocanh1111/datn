FROM apache/airflow:2.10.4-python3.10

# Change to root user
USER root

# Set the working directory (optional, just for organizational purposes)
WORKDIR /data_project/airflow-ui

# Copy the entrypoint script
COPY entrypoint.sh /entrypoint.sh

# Expose the port (Optinal, just for reference, already specified in docker-compose file)
EXPOSE 8080

# Change the permission of the entrypoint script
RUN chmod +wrx /entrypoint.sh

# Run the entrypoint script
ENTRYPOINT [ "/entrypoint.sh" ]
CMD [ "webserver" ]
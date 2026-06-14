FROM apache/superset:4.1.1

# Change to root user
USER root

# Set the working directory (optional, just for organizational purposes)
COPY ./superset-init.sh /app/superset-init.sh
# COPY trino connections for superset
COPY predefined-database.json /app/predefined-database.json

# Install the required packages and change the permission of the entrypoint script
RUN pip install sqlalchemy-trino && \
    chmod +rwx /app/superset-init.sh

# Expose the port (Optinal, just for reference, already specified in docker-compose file)
EXPOSE 8088

# Run the entrypoint script
ENTRYPOINT ["./superset-init.sh"]

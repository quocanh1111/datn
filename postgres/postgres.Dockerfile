FROM postgres:17.2-alpine3.21

# Set the working directory (optional, just for organizational purposes)
WORKDIR /data_project/postgres

# Copy your initialization script into the `/docker-entrypoint-initdb.d` directory
COPY ./init-db.sh /docker-entrypoint-initdb.d/

# Ensure all custom scripts are executable
RUN chmod +x /docker-entrypoint-initdb.d/*.sh
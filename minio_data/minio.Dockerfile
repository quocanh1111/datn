FROM minio/minio:latest
WORKDIR /datn
EXPOSE 9000 9001
CMD ["server", "/datn/data", "--console-address", ":9001"]
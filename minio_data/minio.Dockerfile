# Dùng bản mới nhất để tận dụng các cải tiến về tốc độ I/O
FROM minio/minio:latest

WORKDIR /data_project/minio
EXPOSE 9000 9001

# Lệnh khởi chạy vẫn giữ nguyên
CMD [ "server", "/data_project/data", "--console-address", ":9001"]
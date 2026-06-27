FROM python-base:latest
USER root

# Python deps for Bronze ingestion
RUN pip3 install --no-cache-dir --ignore-installed \
    boto3 \
    pyarrow \
    minio \
    faker

WORKDIR /datn/ingestion
EXPOSE 8888
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''"]

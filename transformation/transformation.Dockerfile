FROM python-base:latest
USER root

# Additional Python deps for S3/Iceberg
RUN pip3 install --no-cache-dir --ignore-installed \
    boto3 \
    pyarrow \
    minio

# Symlink PySpark as SPARK_HOME
RUN ln -s $(python3 -c "import pyspark; import os; print(os.path.dirname(pyspark.__file__))") /opt/spark

# Spark environment
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Copy JARs into Spark
COPY jars/*.jar /opt/spark/jars/

WORKDIR /datn/transformation
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]

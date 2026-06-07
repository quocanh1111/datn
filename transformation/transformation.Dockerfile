FROM apache/spark:3.5.7

WORKDIR /data_project/transformation/

USER root

# Install Python, pip, and wget
RUN apt-get update && \
    apt-get install -y python3 python3-pip wget && \
    rm -rf /var/lib/apt/lists/*

# Set Python path for PySpark
ENV PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"

# Copy requirements.txt and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Create Spark conf directory if missing
RUN mkdir -p $SPARK_HOME/conf

# Copy your updated spark-defaults.conf (must include spark.sql.extensions)
COPY ./spark-defaults.conf $SPARK_HOME/conf

EXPOSE 8888 7070 4040

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''"]
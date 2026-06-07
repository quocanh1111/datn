# Data Engineering Tech Stack: Learn Modern Data Engineering Tools

This repository provides a comprehensive environment to learn and practice modern data engineering concepts using Docker Compose. With this setup, you'll gain hands-on experience working with some of the most popular tools in the data engineering ecosystem. Whether you’re a beginner or looking to expand your skills, this tech stack offers everything you need to work with ETL/ELT pipelines, data lakes, querying, and visualization.

## What You Will Learn

- **Apache Spark**: Learn distributed data processing with one of the most powerful engines for large-scale data transformations.
- **Apache Iceberg**: Understand how to work with Iceberg tables for managing large datasets in data lakes with schema evolution and time travel.
- **Project Nessie**: Explore version control for your data lakehouse, allowing you to track changes to datasets just like Git for code.
- **Apache Airflow**: Master workflow orchestration and scheduling for complex ETL/ELT pipelines.
- **Trino**: Query your data from multiple sources (MinIO, PostgreSQL, Iceberg) with a fast federated SQL engine.
- **Apache Superset**: Create interactive dashboards and visualizations to analyze and present your data.
- **MinIO**: Learn about object storage and how it integrates with modern data pipelines, serving as your S3-compatible storage layer. (Using this architecture for a NYC Taxi Data Project: [https://lnkd.in/gWMj6RS7](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). You'll be seeing the bucket name as "nyc-project")
- **PostgreSQL**: Use a relational database for metadata management and storing structured data.

<p align='center'>
  <img src='https://github.com/user-attachments/assets/63e5fab3-a005-40a4-af55-40dd6472ecfe')
</p>

---


## Table of Contents
- [Overview](#overview)
- [What You Will Learn](#what-you-will-learn)
- [Services](#services)
  - [MinIO](#minio-object-storage)
  - [Airflow](#airflow)
  - [Extraction and Transformation](#extraction-and-transformation)
  - [PostgreSQL](#postgresql)
  - [Project Nessie](#project-nessie)
  - [Trino](#trino)
  - [Superset](#superset)
- [How to Run](#how-to-run)
- [Learning Objectives for Each Tool](#learning-objectives-for-each-tool)
- [Environment Variables](#environment-variables)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## Services

### MinIO (Object Storage)
- **Purpose**: Acts as an S3-compatible object storage layer for raw and processed data.
- **What You’ll Learn**:
  - Uploading and managing files via the MinIO Console.
  - Using MinIO as a source and destination for ETL pipelines.
- **Console URL**: [http://localhost:9001](http://localhost:9001)

### Airflow
- **Purpose**: Workflow orchestration and ETL pipeline management.
- **What You’ll Learn**:
  - Building and scheduling DAGs (Directed Acyclic Graphs) to automate workflows.
  - Managing and monitoring pipelines through the Airflow web interface.
- **Web Interface URL**: [http://localhost:8081](http://localhost:8081)

### Extraction and Transformation
- **Purpose**: Run Spark and custom Python scripts for data ingestion and transformation.
- **What You’ll Learn**:
  - Writing Spark jobs to process large-scale datasets.
  - Using Python scripts to ingest and transform data into MinIO or PostgreSQL.

### PostgreSQL
- **Purpose**: A relational database for storing metadata, structured data, and managing transactions.
- **What You’ll Learn**:
  - Querying relational datasets using SQL.
  - Storing and retrieving structured data for analysis or visualization.

### Project Nessie
- **Purpose**: Version control for data lakes and Iceberg tables.
- **What You’ll Learn**:
  - Creating branches and commits for data changes.
  - Rolling back or time-traveling to previous versions of datasets.
- **API URL**: [http://localhost:19120](http://localhost:19120)

### Trino
- **Purpose**: A federated query engine for SQL-based exploration of multiple data sources.
- **What You’ll Learn**:
  - Querying data stored in MinIO, PostgreSQL, and Iceberg tables.
  - Using SQL to join data from different sources.
- **Web Interface URL**: [http://localhost:8080](http://localhost:8080)

### Superset
- **Purpose**: Visualization and dashboarding tool for creating insights from data.
- **What You’ll Learn**:
  - Building interactive dashboards connected to Trino and PostgreSQL.
  - Analyzing data through visualizations.
- **Dashboard URL**: [http://localhost:8088](http://localhost:8088)

---

## How to Run

1. **Clone the Repository**  
   ```bash
   git clone https://github.com/username/dataengineering-tech-stack.git
   cd dataengineering-tech-stack
   ```
2. **Set Environment Variables**
  - Fill in your configurations in .env/airflow.env, .env/minio.env, .env/postgres.env, etc.

3. **Build & Start Services**
    ``` bash
      docker-compose up -d --build
    ```

4. **Access Servies**
 - MinIO Console: http://localhost:9001
 - Airflow Web UI: http://localhost:8081
 - PostgreSQL: localhost:5432
 - Nessie: http://localhost:19120
 - Trino: http://localhost:8080
 - Superset: http://localhost:8088

---

## Learning Objectives for Each Tool

### Spark
- **Understand how distributed computing works**: Learn how Apache Spark processes large datasets in parallel across multiple nodes, making it an essential tool for handling big data efficiently.
- **Write transformations for ETL pipelines on large-scale datasets**: Build and execute transformations that extract, clean, and load data into storage or analytics layers.

### Iceberg
- **Work with Iceberg tables for schema evolution and time travel**: Gain experience managing dataset changes over time without breaking downstream dependencies.
- **Manage partitions for optimized querying**: Use Iceberg's built-in partitioning to improve query performance on large datasets.

### Project Nessie
- **Learn to implement Git-like workflows for datasets**: Track changes, create branches, and roll back changes to maintain consistency in your data pipelines.
- **Manage branches, tags, and commits for Iceberg tables**: Use Nessie to version-control datasets and simplify collaboration across teams.

### Airflow
- **Build workflows that orchestrate Spark, MinIO, and PostgreSQL**: Automate complex data workflows involving multiple tools and dependencies.
- **Monitor and troubleshoot DAG executions**: Learn to manage and debug Directed Acyclic Graphs (DAGs) for efficient task scheduling.

### Trino
- **Query heterogeneous data sources (e.g., MinIO, PostgreSQL) in a unified SQL layer**: Use Trino to query structured and unstructured data seamlessly across multiple backends.
- **Analyze Iceberg tables and large datasets efficiently**: Leverage Trino's SQL engine to perform ad hoc analysis or integrate with BI tools.

### Superset
- **Build visualizations and interactive dashboards**: Create engaging charts and dashboards to derive insights from your data.
- **Connect Superset to Trino and PostgreSQL for real-time insights**: Visualize data in near real-time to support decision-making.

### MinIO
- **Store and retrieve files in an S3-compatible system**: Manage object storage for raw and processed data in a local or cloud environment.
- **Integrate MinIO with Spark and Airflow pipelines**: Use MinIO as a central hub for ingesting, transforming, and exporting data in your workflows.

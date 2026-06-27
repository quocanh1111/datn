#!/usr/bin/env python
# coding: utf-8

# # 02 - Gold Layer Transformation
# 
# **Purpose:** Build Gold star schema from Silver Iceberg tables.
# 
# **Sources:** Silver Iceberg tables via Nessie
# 
# **Targets:** Gold Iceberg tables via Nessie
# ```
# Dimensions:
#   gold.dim_date
#   gold.dim_channel
#   gold.dim_platform
#   gold.dim_distributor
#   gold.dim_artist
#   gold.dim_content_asset
#   gold.dim_contributor
# 
# Facts:
#   gold.fact_youtube_revenue_daily
#   gold.fact_distributor_revenue_monthly
#   gold.fact_content_revenue_unified
#   gold.fact_contributor_revenue
# ```

# ## 0. Spark Session

# In[1]:


import os

# Force JDK 11
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["PATH"] = "/usr/lib/jvm/java-11-openjdk-amd64/bin:" + os.environ["PATH"]

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

MINIO_ENDPOINT   = "http://storage:9000"
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "root")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "password")
NESSIE_URI       = "http://nessie:19120/api/v2"

spark = (
    SparkSession.builder
    .appName("Gold-Contributor-Fix")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
    .config("spark.sql.catalog.nessie",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.nessie.uri",          NESSIE_URI)
    .config("spark.sql.catalog.nessie.ref",          "main")
    .config("spark.sql.catalog.nessie.warehouse",    "s3a://gold/")
    .config("spark.sql.catalog.nessie.authentication.type", "NONE")
    .config("spark.sql.catalog.nessie.io-impl",
            "org.apache.iceberg.hadoop.HadoopFileIO")
    .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.driver.memory",           "1g")
    .config("spark.executor.memory",         "1g")
    .config("spark.driver.maxResultSize",    "256m")
    .config("spark.sql.shuffle.partitions",  "10")
    # ── GC fix ───────────────────────────────────────────────────────
    .config("spark.driver.extraJavaOptions",
            "-XX:+UseSerialGC")
    .config("spark.executor.extraJavaOptions",
            "-XX:+UseSerialGC")
    # ── Corrupt Parquet stats fix ─────────────────────────────────────
    .config("spark.sql.parquet.filterPushdown",               "false")
    .config("spark.hadoop.parquet.filter.statistics.enabled", "false")
    .config("spark.sql.iceberg.vectorization.enabled",        "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print(f"Spark {spark.version} ready")


# ## 1. Create Gold Namespace

# In[2]:


spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
spark.sql("SHOW NAMESPACES IN nessie").show()
print("Gold namespace ready")


# ## Helper

# In[3]:


def write_gold_table(df, table_name, partition_by=None):
    full_name = f"nessie.gold.{table_name}"
    writer = df.writeTo(full_name).using("iceberg")
    if partition_by:
        writer = writer.partitionedBy(partition_by)
    writer.createOrReplace()
    count = spark.table(full_name).count()
    print(f"  → nessie.gold.{table_name} ({count:,} rows)")
    return count

print("Helper ready")


# ## 2. Dimension Tables

# In[4]:


# ── dim_date ──────────────────────────────────────────────────────────
print("Building dim_date...")

from pyspark.sql.functions import explode, sequence, to_date, lit

date_df = spark.sql("""
    SELECT explode(sequence(
        to_date('2024-01-01'),
        to_date('2026-12-31'),
        interval 1 day
    )) AS full_date
""")

dim_date = date_df.select(
    F.date_format(F.col("full_date"), "yyyyMMdd").cast(IntegerType()).alias("date_key"),
    F.col("full_date"),
    F.dayofmonth(F.col("full_date")).alias("day"),
    F.weekofyear(F.col("full_date")).alias("week"),
    F.month(F.col("full_date")).alias("month"),
    F.quarter(F.col("full_date")).alias("quarter"),
    F.year(F.col("full_date")).alias("year"),
    F.date_format(F.col("full_date"), "MMMM").alias("month_name"),
    F.when(F.dayofweek(F.col("full_date")).isin([1, 7]), True)
     .otherwise(False).alias("is_weekend")
)
write_gold_table(dim_date, "dim_date")


# In[5]:


# ── Simple dims from Silver ───────────────────────────────────────────
print("Building simple dims...")

# dim_platform
write_gold_table(
    spark.table("nessie.silver.platforms"),
    "dim_platform"
)

# dim_distributor
write_gold_table(
    spark.table("nessie.silver.distributors"),
    "dim_distributor"
)

# dim_contributor
write_gold_table(
    spark.table("nessie.silver.contributors"),
    "dim_contributor"
)

# dim_channel
write_gold_table(
    spark.table("nessie.silver.youtube_channels"),
    "dim_channel"
)

# dim_artist
write_gold_table(
    spark.table("nessie.silver.artists"),
    "dim_artist"
)

print("\nSimple dims done.")


# In[6]:


# ── dim_content_asset (unified VIDEO + TRACK) ─────────────────────────
print("Building dim_content_asset...")

# VIDEO assets from youtube_videos
video_assets = spark.table("nessie.silver.youtube_videos").select(
    F.col("asset_id"),
    F.lit("VIDEO").alias("asset_type"),
    F.col("video_title").alias("asset_title"),
    F.col("video_id"),
    F.lit(None).cast(StringType()).alias("track_id"),
    F.lit(None).cast(StringType()).alias("recording_mbid"),
    F.lit(None).cast(StringType()).alias("release_mbid"),
    F.col("channel_id"),
    F.lit(None).cast(StringType()).alias("artist_id"),
    F.lit(None).cast(StringType()).alias("distributor_id"),
    F.col("project_name"),
    F.lit(None).cast(StringType()).alias("label_name"),
    F.col("upload_date").alias("release_date"),
    F.lit("ACTIVE").alias("active_status")
)

# TRACK assets from tracks
track_assets = spark.table("nessie.silver.tracks").select(
    F.col("asset_id"),
    F.lit("TRACK").alias("asset_type"),
    F.col("track_title").alias("asset_title"),
    F.lit(None).cast(StringType()).alias("video_id"),
    F.col("track_id"),
    F.col("recording_mbid"),
    F.col("release_mbid"),
    F.lit(None).cast(StringType()).alias("channel_id"),
    F.col("artist_id"),
    F.col("distributor_id"),
    F.col("project_name"),
    F.col("label_name"),
    F.col("release_date"),
    F.col("active_status")
)

dim_content_asset = video_assets.union(track_assets)
write_gold_table(dim_content_asset, "dim_content_asset")

print("dim_content_asset done.")


# ## 3. Fact Tables

# In[7]:


# ── fact_youtube_revenue_daily ────────────────────────────────────────
print("Building fact_youtube_revenue_daily...")

yt_views   = spark.table("nessie.silver.youtube_view_daily")
yt_rate    = spark.table("nessie.silver.youtube_rate")
yt_videos  = spark.table("nessie.silver.youtube_videos")

fact_yt_revenue = (
    yt_views
    # Join with rate table
    .join(F.broadcast(yt_rate), on="country", how="left")
    # Join with videos to get asset_id and channel_id
    .join(F.broadcast(yt_videos.select("video_id", "asset_id", "channel_id")),
          on="video_id", how="left")
    .select(
        F.date_format(F.col("view_date"), "yyyyMMdd")
         .cast(IntegerType()).alias("date_key"),
        F.col("asset_id"),
        F.col("video_id"),
        F.col("channel_id"),
        F.col("country"),
        F.col("views"),
        F.col("rate_per_view_usd"),
        # Revenue formula: views × rate
        (F.col("views") * F.col("rate_per_view_usd"))
         .cast(DoubleType()).alias("youtube_revenue_usd")
    )
    .filter(F.col("asset_id").isNotNull())
)

write_gold_table(fact_yt_revenue, "fact_youtube_revenue_daily", partition_by="date_key")
print("fact_youtube_revenue_daily done.")


# In[8]:


# ── fact_distributor_revenue_monthly ──────────────────────────────────
print("Building fact_distributor_revenue_monthly...")

dist_rev  = spark.table("nessie.silver.distributor_revenue_monthly")
tracks    = spark.table("nessie.silver.tracks").select("track_id", "asset_id")

fact_dist_revenue = (
    dist_rev
    .join(F.broadcast(tracks), on="track_id", how="left")
    .select(
        F.col("report_month").alias("month_key"),
        F.col("asset_id"),
        F.col("track_id"),
        F.col("artist_id"),
        F.col("distributor_id"),
        F.col("platform_id"),
        F.col("country"),
        F.col("streams"),
        F.col("revenue_original"),
        F.col("currency"),
        F.col("exchange_rate"),
        F.col("revenue_usd")
    )
    .filter(F.col("asset_id").isNotNull())
)

write_gold_table(fact_dist_revenue, "fact_distributor_revenue_monthly", partition_by="month_key")
print("fact_distributor_revenue_monthly done.")


# In[9]:


# ── fact_content_revenue_unified ──────────────────────────────────────
print("Building fact_content_revenue_unified...")

# YouTube side
yt_unified = spark.table("nessie.gold.fact_youtube_revenue_daily").select(
    F.col("date_key").cast(StringType()).alias("revenue_period"),
    F.lit("DAILY").alias("period_type"),
    F.lit("YOUTUBE").alias("source_type"),
    F.col("asset_id"),
    F.lit("VIDEO").alias("asset_type"),
    F.lit("PLT_YTB").alias("platform_id"),
    F.lit(None).cast(StringType()).alias("distributor_id"),
    F.col("country"),
    F.col("views"),
    F.lit(None).cast(LongType()).alias("streams"),
    F.col("youtube_revenue_usd").alias("revenue_usd")
)

# Distributor side
dist_unified = spark.table("nessie.gold.fact_distributor_revenue_monthly").select(
    F.col("month_key").alias("revenue_period"),
    F.lit("MONTHLY").alias("period_type"),
    F.lit("DISTRIBUTOR").alias("source_type"),
    F.col("asset_id"),
    F.lit("TRACK").alias("asset_type"),
    F.col("platform_id"),
    F.col("distributor_id"),
    F.col("country"),
    F.lit(None).cast(LongType()).alias("views"),
    F.col("streams"),
    F.col("revenue_usd")
)

# Union both sources
fact_unified = yt_unified.union(dist_unified)
write_gold_table(fact_unified, "fact_content_revenue_unified", partition_by="source_type")
print("fact_content_revenue_unified done.")


# In[2]:


# ── fact_contributor_revenue ──────────────────────────────────────────
unified = spark.table("nessie.gold.fact_content_revenue_unified")
mapping = spark.table("nessie.silver.content_contributor_mapping")

asset_rev = (
    unified
    .groupBy("revenue_period", "period_type", "source_type", "asset_id")
    .agg(F.sum("revenue_usd").alias("total_asset_revenue_usd"))
)
asset_rev.write.mode("overwrite").parquet("s3a://silver/tmp/asset_rev/")
print("Intermediate written")

asset_rev_clean = spark.read.parquet("s3a://silver/tmp/asset_rev/")
print(f"Asset revenue rows: {asset_rev_clean.count():,}")

fact_contributor = (
    asset_rev_clean
    .join(F.broadcast(mapping), on="asset_id", how="inner")
    .select(
        "revenue_period", "period_type", "source_type",
        "asset_id", "contributor_id", "role_in_asset",
        F.col("total_asset_revenue_usd").alias("revenue_usd"),
        "allocation_percent",
        (F.col("total_asset_revenue_usd") * F.col("allocation_percent"))
         .cast(DoubleType()).alias("allocated_revenue_usd")
    )
)

fact_contributor.writeTo("nessie.gold.fact_contributor_revenue") \
    .using("iceberg") \
    .partitionedBy("source_type") \
    .createOrReplace()

print("fact_contributor_revenue done ✅")("Building fact_contributor_revenue...")

# First materialize asset revenue to avoid large lazy evaluation chain
asset_revenue = (
    spark.table("nessie.gold.fact_content_revenue_unified")
    .groupBy("revenue_period", "period_type", "source_type", "asset_id")
    .agg(F.sum("revenue_usd").alias("total_asset_revenue_usd"))
    .cache()  # ← cache intermediate result
)

asset_revenue.count()  # ← force materialization
print(f"Asset revenue rows: {asset_revenue.count():,}")

mapping = spark.table("nessie.silver.content_contributor_mapping").cache()
mapping.count()  # ← force materialization
print(f"Mapping rows: {mapping.count():,}")

# Now join on cached DataFrames
fact_contributor = (
    asset_revenue
    .join(F.broadcast(mapping), on="asset_id", how="inner")
    .select(
        F.col("revenue_period"),
        F.col("period_type"),
        F.col("source_type"),
        F.col("asset_id"),
        F.col("contributor_id"),
        F.col("role_in_asset"),
        F.col("total_asset_revenue_usd").alias("revenue_usd"),
        F.col("allocation_percent"),
        (F.col("total_asset_revenue_usd") * F.col("allocation_percent"))
         .cast(DoubleType()).alias("allocated_revenue_usd")
    )
)

write_gold_table(fact_contributor, "fact_contributor_revenue", partition_by="source_type")

print("fact_contributor_revenue done.")


# ## 4. Validate Gold Layer

# In[3]:


gold_tables = [
    # Dims
    "dim_date", "dim_platform", "dim_distributor",
    "dim_contributor", "dim_channel", "dim_artist",
    "dim_content_asset",
    # Facts
    "fact_youtube_revenue_daily",
    "fact_distributor_revenue_monthly",
    "fact_content_revenue_unified",
    "fact_contributor_revenue"
]

print(f"{'Table':<40} {'Rows':>12}")
print("-" * 55)
total = 0
for t in gold_tables:
    try:
        count = spark.table(f"nessie.gold.{t}").count()
        total += count
        marker = "← dim" if t.startswith("dim") else "← fact"
        print(f"  gold.{t:<35} {count:>10,}  {marker}")
    except Exception as e:
        print(f"  gold.{t:<35} {'ERROR':>10}")
print("-" * 55)
print(f"  {'TOTAL':<38} {total:>10,}")


# In[4]:


# Business metrics validation
print("=" * 60)
print("BUSINESS METRICS VALIDATION")
print("=" * 60)

# Total revenue
total_revenue = spark.table("nessie.gold.fact_content_revenue_unified") \
    .agg(F.sum("revenue_usd")).collect()[0][0]
print(f"Total revenue USD      : ${total_revenue:,.2f}")

# Revenue by source
print("\nRevenue by source:")
spark.table("nessie.gold.fact_content_revenue_unified") \
    .groupBy("source_type") \
    .agg(F.sum("revenue_usd").alias("revenue_usd")) \
    .orderBy(F.desc("revenue_usd")) \
    .show()

# Top 10 contributors
print("Top 10 contributors by allocated revenue:")
spark.table("nessie.gold.fact_contributor_revenue") \
    .groupBy("contributor_id") \
    .agg(F.sum("allocated_revenue_usd").alias("total_allocated")) \
    .orderBy(F.desc("total_allocated")) \
    .limit(10) \
    .show()

# Revenue by month (distributor)
print("Distributor revenue by month:")
spark.table("nessie.gold.fact_distributor_revenue_monthly") \
    .groupBy("month_key") \
    .agg(
        F.sum("streams").alias("total_streams"),
        F.sum("revenue_usd").alias("total_revenue")
    ) \
    .orderBy("month_key") \
    .show(truncate=False)


# In[5]:


import requests

response = requests.get("http://nessie:19120/api/v2/trees/main/entries")
data = response.json()

gold_tables = [
    ".".join(e.get("name", {}).get("elements", []))
    for e in data.get("entries", [])
    if "gold" in ".".join(e.get("name", {}).get("elements", []))
]

print(f"Gold tables ({len(gold_tables)}):")
for t in sorted(gold_tables):
    print(f"  ✅ {t}")


# In[ ]:





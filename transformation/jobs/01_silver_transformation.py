#!/usr/bin/env python
# coding: utf-8

# # 01 - Silver Layer Transformation
# 
# **Purpose:** Transform Bronze data into Silver layer using Spark + Iceberg + Nessie.
# 
# **Sources (Bronze):**
# - `bronze/listenbrainz/` → 214M raw listening events
# - `bronze/tracks/` → HG Media track catalog (real MBIDs)
# - `bronze/artists/` → HG Media artist roster (real MBIDs)
# - `bronze/contributors/` → HG Media staff
# - `bronze/platforms/` → Streaming platforms
# - `bronze/distributors/` → Distributors
# - `bronze/youtube_channels/` → YouTube channels
# - `bronze/youtube_videos/` → YouTube videos
# - `bronze/youtube_rate/` → Revenue rates by country
# - `bronze/youtube_view_daily/` → Daily view counts
# - `bronze/distributor_revenue_monthly/` → Monthly revenue reports
# - `bronze/content_contributor_mapping/` → Asset-contributor mapping
# 
# **Targets (Silver - Iceberg via Nessie):**
# - `silver.tracks`
# - `silver.artists`
# - `silver.contributors`
# - `silver.platforms`
# - `silver.distributors`
# - `silver.youtube_channels`
# - `silver.youtube_videos`
# - `silver.youtube_rate`
# - `silver.youtube_view_daily`
# - `silver.distributor_revenue_monthly`
# - `silver.content_contributor_mapping`
# - `silver.listenbrainz_stream_monthly` ← aggregated from 214M rows

# ## 0. Spark Session with Nessie + Iceberg + MinIO

# In[10]:


import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os

# ── MinIO credentials ────────────────────────────────────────────────
MINIO_ENDPOINT   = "http://storage:9000"
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "root")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "password")

# ── Nessie config ─────────────────────────────────────────────────────
NESSIE_URI       = "http://nessie:19120/api/v2"
NESSIE_BRANCH    = "main"
WAREHOUSE        = "s3a://silver/"
INGESTED_DATE    = "2026-06-26"

spark = (
    SparkSession.builder
    .appName("HGMedia-Silver-Transformation")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
    # ── Nessie catalog ────────────────────────────────────────────────
    .config("spark.sql.catalog.nessie",              "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.nessie.uri",          NESSIE_URI)
    .config("spark.sql.catalog.nessie.ref",          NESSIE_BRANCH)
    .config("spark.sql.catalog.nessie.warehouse",    WAREHOUSE)
    .config("spark.sql.catalog.nessie.authentication.type", "NONE")
    .config("spark.sql.catalog.nessie.io-impl",      "org.apache.iceberg.hadoop.HadoopFileIO")
    # ── S3A / MinIO ───────────────────────────────────────────────────
    .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    # ── Performance ───────────────────────────────────────────────────
    .config("spark.driver.memory",   "1g")
    .config("spark.executor.memory", "1g")
    .config("spark.driver.maxResultSize", "512m")
    .config("spark.sql.shuffle.partitions", "20")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print(f"Spark version : {spark.version}")
print(f"Nessie URI    : {NESSIE_URI}")
print(f"Warehouse     : {WAREHOUSE}")


# ## 1. Test Nessie Connection

# In[2]:


# Create silver namespace in Nessie
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
spark.sql("SHOW NAMESPACES IN nessie").show()
print("Nessie connection OK")


# ## Helper Functions

# In[3]:


import boto3
import io
import pandas as pd

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1"
)

def read_bronze_csv(prefix, filename):
    """Read a CSV from Bronze bucket into Spark DataFrame."""
    response = s3.get_object(Bucket="bronze", Key=f"{prefix}{filename}")
    pdf = pd.read_csv(io.BytesIO(response["Body"].read()))
    return spark.createDataFrame(pdf)

def write_silver_table(df, table_name, partition_by=None):
    """Write DataFrame to Silver Iceberg table via Nessie."""
    full_name = f"nessie.silver.{table_name}"
    writer = df.writeTo(full_name).using("iceberg")
    if partition_by:
        writer = writer.partitionedBy(partition_by)
    writer.createOrReplace()
    count = spark.table(full_name).count()
    print(f"Written → {full_name} ({count:,} rows)")
    return count

print("Helpers ready")


# ## 2. Silver Master Tables (Context Layer)

# In[4]:


# ── 2a. Platforms ─────────────────────────────────────────────────────
platforms_df = read_bronze_csv("platforms/", "platforms.csv")
platforms_silver = platforms_df.select(
    F.col("platform_id").cast(StringType()),
    F.trim(F.col("platform_name")).alias("platform_name"),
    F.upper(F.col("platform_type")).alias("platform_type")
).dropDuplicates(["platform_id"])
write_silver_table(platforms_silver, "platforms")

# ── 2b. Distributors ──────────────────────────────────────────────────
dist_df = read_bronze_csv("distributors/", "distributors.csv")
dist_silver = dist_df.select(
    F.col("distributor_id").cast(StringType()),
    F.trim(F.col("distributor_name")).alias("distributor_name"),
    F.col("payment_cycle").cast(StringType()),
    F.upper(F.col("active_status")).alias("active_status")
).dropDuplicates(["distributor_id"])
write_silver_table(dist_silver, "distributors")

# ── 2c. Contributors ──────────────────────────────────────────────────
contrib_df = read_bronze_csv("contributors/", "contributors.csv")
contrib_silver = contrib_df.select(
    F.col("contributor_id").cast(StringType()),
    F.trim(F.col("contributor_name")).alias("contributor_name"),
    F.trim(F.col("team")).alias("team"),
    F.trim(F.col("default_role")).alias("default_role"),
    F.upper(F.col("active_status")).alias("active_status")
).dropDuplicates(["contributor_id"])
write_silver_table(contrib_silver, "contributors")

print("\nReference tables done.")


# In[5]:


# ── 2d. Artists ───────────────────────────────────────────────────────
artists_df = read_bronze_csv("artists/", "artists.csv")
artists_silver = artists_df.select(
    F.col("artist_id").cast(StringType()),
    F.trim(F.col("artist_name")).alias("artist_name"),
    F.col("distributor_id").cast(StringType()),
    F.trim(F.col("label_name")).alias("label_name"),
    F.trim(F.col("project_name")).alias("project_name"),
    F.upper(F.col("active_status")).alias("active_status"),
    F.to_date(F.col("created_date"), "yyyy-MM-dd").alias("created_date")
).dropDuplicates(["artist_id"]).filter(F.col("artist_id").isNotNull())
write_silver_table(artists_silver, "artists")

# ── 2e. Tracks ────────────────────────────────────────────────────────
tracks_df = read_bronze_csv("tracks/", "tracks.csv")
tracks_silver = tracks_df.select(
    F.col("track_id").cast(StringType()),
    F.trim(F.col("track_title")).alias("track_title"),
    F.col("asset_id").cast(StringType()),
    F.col("isrc").cast(StringType()),
    F.col("recording_mbid").cast(StringType()),
    F.col("release_mbid").cast(StringType()),
    F.trim(F.col("release_name")).alias("release_name"),
    F.col("artist_id").cast(StringType()),
    F.col("distributor_id").cast(StringType()),
    F.trim(F.col("label_name")).alias("label_name"),
    F.trim(F.col("project_name")).alias("project_name"),
    F.to_date(F.col("release_date"), "yyyy-MM-dd").alias("release_date"),
    F.upper(F.col("active_status")).alias("active_status")
).dropDuplicates(["track_id"]).filter(F.col("track_id").isNotNull())
write_silver_table(tracks_silver, "tracks")

print("\nArtists and tracks done.")


# In[6]:


# ── 2f. YouTube Channels ──────────────────────────────────────────────
channels_df = read_bronze_csv("youtube_channels/", "youtube_channels.csv")
channels_silver = channels_df.select(
    F.col("channel_id").cast(StringType()),
    F.trim(F.col("channel_name")).alias("channel_name"),
    F.trim(F.col("owner_team")).alias("owner_team"),
    F.to_date(F.col("created_date"), "yyyy-MM-dd").alias("created_date"),
    F.upper(F.col("active_status")).alias("active_status")
).dropDuplicates(["channel_id"])
write_silver_table(channels_silver, "youtube_channels")

# ── 2g. YouTube Videos ────────────────────────────────────────────────
videos_df = read_bronze_csv("youtube_videos/", "youtube_videos.csv")
videos_silver = videos_df.select(
    F.col("video_id").cast(StringType()),
    F.trim(F.col("video_title")).alias("video_title"),
    F.col("asset_id").cast(StringType()),
    F.col("channel_id").cast(StringType()),
    F.col("track_id").cast(StringType()),
    F.trim(F.col("project_name")).alias("project_name"),
    F.trim(F.col("content_type")).alias("content_type"),
    F.to_date(F.col("upload_date"), "yyyy-MM-dd").alias("upload_date"),
    F.col("editor_code").cast(StringType())
).dropDuplicates(["video_id"])
write_silver_table(videos_silver, "youtube_videos")

# ── 2h. YouTube Rate ──────────────────────────────────────────────────
rate_df = read_bronze_csv("youtube_rate/", "youtube_rate.csv")
rate_silver = rate_df.select(
    F.col("country").cast(StringType()),
    F.trim(F.col("country_name")).alias("country_name"),
    F.col("rate_per_view_usd").cast(DoubleType()),
    F.to_date(F.col("effective_from"), "yyyy-MM-dd").alias("effective_from"),
    F.to_date(F.col("effective_to"), "yyyy-MM-dd").alias("effective_to")
).dropDuplicates(["country"])
write_silver_table(rate_silver, "youtube_rate")

print("\nYouTube tables done.")


# In[7]:


# ── 2i. Content Contributor Mapping ──────────────────────────────────
mapping_df = read_bronze_csv("content_contributor_mapping/", "content_contributor_mapping.csv")
mapping_silver = mapping_df.select(
    F.col("asset_id").cast(StringType()),
    F.col("contributor_id").cast(StringType()),
    F.trim(F.col("role_in_asset")).alias("role_in_asset"),
    F.upper(F.col("allocation_method")).alias("allocation_method"),
    F.col("allocation_percent").cast(DoubleType())
).dropDuplicates(["asset_id", "contributor_id"])\
 .filter(F.col("asset_id").isNotNull() & F.col("contributor_id").isNotNull())
write_silver_table(mapping_silver, "content_contributor_mapping")

print("\nMapping table done.")


# ## 3. Silver Transactional Tables

# In[9]:


# ── 3a. YouTube View Daily ────────────────────────────────────────────
yt_views_df = read_bronze_csv("youtube_view_daily/", "youtube_view_daily.csv")
yt_views_silver = yt_views_df.select(
    F.to_date(F.col("view_date"), "yyyy-MM-dd").alias("view_date"),
    F.col("video_id").cast(StringType()),
    F.upper(F.col("country")).alias("country"),
    F.col("views").cast(LongType())
).filter(
    F.col("video_id").isNotNull() &
    F.col("views").isNotNull() &
    (F.col("views") >= 0)
).dropDuplicates(["view_date", "video_id", "country"])
write_silver_table(yt_views_silver, "youtube_view_daily", partition_by="view_date")

print("\nYouTube view daily done.")


# In[ ]:


# ── 3b. Distributor Revenue Monthly ───────────────────────────────────
dist_rev_df = read_bronze_csv("distributor_revenue_monthly/", "distributor_revenue_monthly.csv")
dist_rev_silver = dist_rev_df.select(
    F.col("report_month").cast(StringType()),
    F.col("distributor_id").cast(StringType()),
    F.col("platform_id").cast(StringType()),
    F.col("artist_id").cast(StringType()),
    F.col("track_id").cast(StringType()),
    F.upper(F.col("country")).alias("country"),
    F.col("streams").cast(LongType()),
    F.col("revenue_original").cast(DoubleType()),
    F.upper(F.col("currency")).alias("currency"),
    F.col("exchange_rate").cast(DoubleType()),
    # Calculate USD revenue
    (F.col("revenue_original") * F.col("exchange_rate"))
        .cast(DoubleType()).alias("revenue_usd")
).filter(
    F.col("track_id").isNotNull() &
    F.col("streams").isNotNull() &
    (F.col("streams") >= 0) &
    (F.col("revenue_original") >= 0)
).dropDuplicates(["report_month", "distributor_id", "platform_id", "track_id", "country"])
write_silver_table(dist_rev_silver, "distributor_revenue_monthly", partition_by="report_month")

print("\nDistributor revenue monthly done.")


# ## 4. ListenBrainz → Silver Stream Monthly
# **This is the core Spark job — filtering 214M rows to HG Media's 500 tracks**

# In[ ]:


# Load HG Media track catalog (our 500 MBIDs)
hg_tracks = spark.table("nessie.silver.tracks").select(
    F.col("track_id").alias("recording_mbid"),
    F.col("asset_id"),
    F.col("artist_id"),
    F.col("distributor_id")
).filter(F.col("active_status") == "ACTIVE")

hg_mbids = set(row.recording_mbid for row in hg_tracks.collect())
print(f"HG Media active tracks: {len(hg_mbids)}")

# Broadcast for efficient join
hg_tracks_broadcast = F.broadcast(hg_tracks)


# In[ ]:


# Read all 100 ListenBrainz partitions from Bronze
lb_df = spark.read.parquet(
    f"s3a://bronze/listenbrainz/ingested_date={INGESTED_DATE}/"
)

print(f"ListenBrainz Bronze rows: {lb_df.count():,}")
lb_df.printSchema()


# In[ ]:


# ── Core transformation ───────────────────────────────────────────────
# 1. Filter to HG Media tracks only
# 2. Clean nulls and duplicates  
# 3. Extract year-month for aggregation
# 4. Join with HG Media catalog
# 5. Aggregate to monthly stream counts

lb_silver = (
    lb_df
    .filter(F.col("recording_mbid").isNotNull())
    .join(F.broadcast(hg_tracks), on="recording_mbid", how="inner")
    # Add listen_minute as a column FIRST, then dropDuplicates on its name
    .withColumn("listen_minute", F.date_trunc("minute", F.col("listened_at")))
    .dropDuplicates(["user_id", "recording_mbid", "listen_minute"])
    .withColumn("listen_month", F.date_format(F.col("listened_at"), "yyyyMM"))
    .groupBy(
        "listen_month",
        "recording_mbid",
        "asset_id",
        "artist_id",
        "distributor_id"
    )
    .agg(
        F.count("*").alias("stream_count"),
        F.countDistinct("user_id").alias("unique_listeners")
    )
)

write_silver_table(
    lb_silver,
    "listenbrainz_stream_monthly",
    partition_by="listen_month"
)

print("\nListenBrainz aggregation complete.")


# ## 5. Validate Silver Tables

# In[ ]:


silver_tables = [
    "platforms", "distributors", "contributors",
    "artists", "tracks",
    "youtube_channels", "youtube_videos", "youtube_rate",
    "youtube_view_daily", "distributor_revenue_monthly",
    "content_contributor_mapping",
    "listenbrainz_stream_monthly"
]

print(f"{'Table':<35} {'Rows':>10}")
print("-" * 48)
total = 0
for t in silver_tables:
    count = spark.table(f"nessie.silver.{t}").count()
    total += count
    print(f"  silver.{t:<27} {count:>10,}")
print("-" * 48)
print(f"  {'TOTAL':<32} {total:>10,}")


# In[ ]:


# Check Nessie commit history
import requests

response = requests.get(
    "http://nessie:19120/api/v2/trees/main/history",
    params={"max-records": 10}
)

data = response.json()

print("Nessie commit history:")
print(f"{'Hash':<12} {'Time':<30} {'Message'}")
print("-" * 70)
for entry in data.get("logEntries", []):
    commit = entry.get("commitMeta", {})
    hash_short = entry.get("commitHash", "")[:10]
    time = commit.get("commitTime", "")
    message = commit.get("message", "")[:40]
    print(f"{hash_short:<12} {time:<30} {message}")


# In[10]:


# Sample from ListenBrainz aggregation
print("ListenBrainz stream monthly sample:")
spark.table("nessie.silver.listenbrainz_stream_monthly") \
    .orderBy(F.desc("stream_count")) \
    .show(10, truncate=False)

print("\nDistributor revenue sample:")
spark.table("nessie.silver.distributor_revenue_monthly") \
    .groupBy("report_month") \
    .agg(
        F.sum("streams").alias("total_streams"),
        F.sum("revenue_usd").alias("total_revenue_usd")
    ) \
    .orderBy("report_month") \
    .show(truncate=False)


# In[ ]:





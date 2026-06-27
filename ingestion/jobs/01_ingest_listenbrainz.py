#!/usr/bin/env python
# coding: utf-8

# # 01 - Ingest ListenBrainz Data to Bronze Layer
# 
# **Purpose:** Read 100 sampled ListenBrainz parquet partitions from local mount and upload to MinIO Bronze bucket via S3 API.
# 
# **Source:** `/datn/raw/listenbrainz/` (100 partitions, ~19GB, ~100M rows)
# 
# **Target:** `s3://bronze/listenbrainz/ingested_date=YYYY-MM-DD/`
# 
# **Schema:**
# ```
# listened_at          timestamp    when the user listened
# created              timestamp    when the record was created  
# user_id              int64        anonymous user identifier
# recording_msid       string       ListenBrainz internal ID
# artist_name          string       raw artist name
# artist_credit_id     int64        MusicBrainz artist credit ID
# release_name         string       album/release name
# release_mbid         string       MusicBrainz release MBID
# recording_name       string       track/song name
# recording_mbid       string       MusicBrainz recording MBID
# artist_credit_mbids  list<string> list of artist MBIDs
# ```

# ## 0. Imports and Configuration

# In[1]:


import boto3
import pyarrow.parquet as pq
import os
import glob
from datetime import datetime
from botocore.exceptions import ClientError

# ── MinIO Connection Config ──────────────────────────────────────────
MINIO_ENDPOINT    = "http://storage:9000"   # container name in docker network
MINIO_ACCESS_KEY  = os.environ.get("MINIO_ROOT_USER", "root")
MINIO_SECRET_KEY  = os.environ.get("MINIO_ROOT_PASSWORD", "password")

# ── Paths ────────────────────────────────────────────────────────────
RAW_DIR           = "/datn/raw/listenbrainz/"
BRONZE_BUCKET     = "bronze"
INGESTED_DATE     = datetime.now().strftime("%Y-%m-%d")
BRONZE_PREFIX     = f"listenbrainz/ingested_date={INGESTED_DATE}/"

print(f"MinIO endpoint : {MINIO_ENDPOINT}")
print(f"Source dir     : {RAW_DIR}")
print(f"Target bucket  : {BRONZE_BUCKET}")
print(f"Target prefix  : {BRONZE_PREFIX}")
print(f"Ingestion date : {INGESTED_DATE}")


# ## 1. Connect to MinIO

# In[2]:


s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1"
)

# Verify connection and bucket exists
try:
    buckets = s3.list_buckets()["Buckets"]
    bucket_names = [b["Name"] for b in buckets]
    print(f"Connected to MinIO. Available buckets: {bucket_names}")
    assert BRONZE_BUCKET in bucket_names, f"Bucket '{BRONZE_BUCKET}' not found!"
    print(f"Target bucket '{BRONZE_BUCKET}' confirmed.")
except Exception as e:
    print(f"Connection failed: {e}")
    raise


# ## 2. Scan Source Files

# In[3]:


# Get all parquet files sorted by partition number
parquet_files = sorted(
    glob.glob(os.path.join(RAW_DIR, "*.parquet")),
    key=lambda x: int(os.path.basename(x).replace(".parquet", ""))
)

print(f"Found {len(parquet_files)} parquet files")
print(f"First : {os.path.basename(parquet_files[0])}")
print(f"Last  : {os.path.basename(parquet_files[-1])}")

# Quick schema check on first file
sample_schema = pq.read_schema(parquet_files[0])
print(f"\nSchema ({len(sample_schema)} columns):")
for field in sample_schema:
    print(f"  {field.name}: {field.type}")


# ## 3. Check Already Ingested Files (Idempotency)

# In[4]:


# List files already in Bronze to skip re-uploading
already_ingested = set()
try:
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=BRONZE_PREFIX)
    for page in pages:
        for obj in page.get("Contents", []):
            filename = os.path.basename(obj["Key"])
            already_ingested.add(filename)
except Exception as e:
    print(f"Could not list existing objects: {e}")

print(f"Already ingested: {len(already_ingested)} files")

# Filter to only files not yet ingested
to_ingest = [
    f for f in parquet_files
    if os.path.basename(f) not in already_ingested
]
print(f"Files to ingest : {len(to_ingest)}")


# ## 4. Ingest to Bronze

# In[5]:


import time

# ── Ingestion Stats ──────────────────────────────────────────────────
stats = {
    "total_files"   : len(to_ingest),
    "uploaded"      : 0,
    "failed"        : 0,
    "total_rows"    : 0,
    "total_bytes"   : 0,
    "failed_files"  : []
}

start_time = time.time()

for i, local_path in enumerate(to_ingest, 1):
    filename   = os.path.basename(local_path)
    s3_key     = f"{BRONZE_PREFIX}{filename}"
    file_size  = os.path.getsize(local_path)

    try:
        # Row count from metadata only - no full read
        pf        = pq.ParquetFile(local_path)
        row_count = pf.metadata.num_rows

        # Upload to MinIO
        s3.upload_file(
            Filename  = local_path,
            Bucket    = BRONZE_BUCKET,
            Key       = s3_key,
            ExtraArgs = {"ContentType": "application/octet-stream"}
        )

        stats["uploaded"]    += 1
        stats["total_rows"]  += row_count
        stats["total_bytes"] += file_size

        elapsed = time.time() - start_time
        print(
            f"[{i:>3}/{stats['total_files']}] {filename} "
            f"| rows: {row_count:>8,} "
            f"| size: {file_size/1024/1024:>7.1f}MB "
            f"| elapsed: {elapsed:.0f}s"
        )

    except Exception as e:
        stats["failed"]       += 1
        stats["failed_files"].append(filename)
        print(f"[{i:>3}/{stats['total_files']}] FAILED {filename}: {e}")

print(f"\n{'='*60}")
print(f"Ingestion complete")
print(f"  Uploaded  : {stats['uploaded']} files")
print(f"  Failed    : {stats['failed']} files")
print(f"  Total rows: {stats['total_rows']:,}")
print(f"  Total size: {stats['total_bytes']/1024/1024/1024:.2f} GB")
print(f"  Duration  : {time.time() - start_time:.0f}s")
if stats["failed_files"]:
    print(f"  Failed    : {stats['failed_files']}")


# ## 5. Verify Bronze Contents

# In[6]:


# List what's now in Bronze
paginator  = s3.get_paginator("list_objects_v2")
pages      = paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=BRONZE_PREFIX)

total_objects = 0
total_size    = 0

for page in pages:
    for obj in page.get("Contents", []):
        total_objects += 1
        total_size    += obj["Size"]

print(f"Bronze bucket contents ({BRONZE_PREFIX}):")
print(f"  Objects : {total_objects}")
print(f"  Size    : {total_size/1024/1024/1024:.2f} GB")
print(f"  Status  : {'OK' if total_objects == len(parquet_files) else 'WARNING - count mismatch'}")


# ## 6. Sample Data Check

# In[7]:


# Read first file directly from MinIO and show sample rows
import pyarrow as pa

first_key = f"{BRONZE_PREFIX}{os.path.basename(parquet_files[0])}"

response = s3.get_object(Bucket=BRONZE_BUCKET, Key=first_key)
body     = response["Body"].read()

buf   = pa.BufferReader(pa.py_buffer(body))
table = pq.read_table(buf)

print(f"Sample from Bronze ({first_key}):")
print(f"  Rows   : {table.num_rows:,}")
print(f"  Columns: {table.column_names}")
print()

# Show 3 sample rows as dict
sample = table.slice(0, 3).to_pydict()
for i in range(3):
    print(f"Row {i+1}:")
    for col in table.column_names:
        print(f"  {col:30s}: {sample[col][i]}")
    print()


# In[ ]:





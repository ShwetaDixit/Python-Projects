import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Initialize Spark Session with GCS, Delta Lake, and BigQuery connectors
# Ensure these jars/packages are supplied to your Dataproc cluster submit command
spark = SparkSession.builder \
    .appName("SchemaEvolutionStream") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 1. READ FROM SOURCE (Example: Cloud Pub/Sub Lite or Cloud Storage Landing Zone)
# For this example, we assume an ingress GCS bucket acting as a streaming source
input_schema_path = "gs://<YOUR_METADATA_BUCKET>/schemas/initial_schema.json"

source_stream = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "gs://<YOUR_METADATA_BUCKET>/offsets/source_schema_tracking") \
    .load("gs://<YOUR_SOURCE_BUCKET>/ingress_events/")

# 2. WRITE TO DELTA LAKE STORAGE (Storage Tier with Auto-Evolution)
# 'mergeSchema' allows Spark to automatically adapt when a required field is dropped (it will append NULLs)
delta_gcs_path = "gs://<YOUR_STORAGE_BUCKET>/delta/silver_events"
checkpoint_path = "gs://<YOUR_METADATA_BUCKET>/offsets/delta_checkpoint"

delta_query = source_stream.writeStream \
    .format("delta") \
    .option("mergeSchema", "true") \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .start(delta_gcs_path)

# 3. STREAM FROM DELTA LAKE TO BIGQUERY
# Reading from the Delta layer allows us to abstract schema changes. 
# The BigQuery Spark connector handles schema updates gracefully when using direct or indirect write modes.
bq_checkpoint_path = "gs://<YOUR_METADATA_BUCKET>/offsets/bq_checkpoint"

bq_query = spark.readStream \
    .format("delta") \
    .load(delta_gcs_path) \
    .writeStream \
    .format("bigquery") \
    .option("checkpointLocation", bq_checkpoint_path) \
    .option("table", "<YOUR_PROJECT>.<YOUR_DATASET>.<YOUR_TARGET_TABLE>") \
    .option("temporaryGcsBucket", "<YOUR_TEMP_BUCKET>") \
    .option("allowFieldAddition", "true") \
    .option("allowFieldRelaxation", "true") \
    .outputMode("append") \
    .start()

# Block until termination
spark.streams.awaitAnyTermination()
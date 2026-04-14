"""
Silver Layer - Transform + Load to S3 and Redshift

SUBTLE SCHEMA ANOMALY: Converts total_amount to a string column for ~8% of rows
by concatenating a currency symbol (e.g., "$123.45" instead of 123.45).
When written to Redshift, the column becomes VARCHAR to accommodate mixed data.
The Silver Redshift load SUCCEEDS because Redshift accepts VARCHAR.

But when Gold reads this from Redshift and tries to do SUM(total_amount),
it fails because you can't aggregate a VARCHAR column numerically.
The DevOps Agent must trace this back to Silver's data corruption.
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, rand, lit, floor, date_add, expr, concat as spark_concat
)
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'bronze_input_path', 'silver_output_path',
    'redshift_conn_name', 'redshift_db', 'redshift_table', 'redshift_temp_dir'
])
spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()
glueContext = GlueContext(spark.sparkContext)

# Security: Validate S3 paths to prevent reading/writing to unintended buckets
bronze_path = args['bronze_input_path']
silver_path = args['silver_output_path']
assert bronze_path.startswith("s3://"), "bronze_input_path must be a valid S3 path"
assert silver_path.startswith("s3://"), "silver_output_path must be a valid S3 path"

print("Silver Layer: Reading from Bronze")
df = spark.read.parquet(bronze_path)

# M1: Schema validation - ensure Bronze data has expected columns and types
EXPECTED_COLUMNS = {"order_id", "customer_id", "product", "quantity", "unit_price", "total_amount", "region", "order_status", "order_date"}
actual_columns = set(df.columns)
missing = EXPECTED_COLUMNS - actual_columns
assert not missing, f"Bronze schema validation failed - missing columns: {missing}"

# Validate total_amount is numeric (not corrupted)
from pyspark.sql.functions import col as _col
non_numeric_count = df.filter(_col("total_amount").cast("double").isNull()).count()
assert non_numeric_count == 0, f"Bronze schema validation failed - {non_numeric_count} non-numeric total_amount values"

# Cost: Cache before count to avoid recomputation during write
df.cache()
initial_count = df.count()
print(f"Silver Layer: Read {initial_count} records from Bronze")

# ---- Legitimate transformations ----
df = df.withColumn("product", expr("upper(product)"))
df = df.withColumn("price_category",
    when(col("unit_price") < 50, "budget")
    .when(col("unit_price") < 200, "mid-range")
    .otherwise("premium")
)

# ---- SCHEMA ANOMALY: Mix string values into total_amount ----
# This "currency formatting" looks like a legitimate transformation
# but it corrupts the column type from numeric to string
df = df.withColumn("total_amount",
    when(rand() < 0.08,
         spark_concat(lit("$"), col("total_amount").cast("string")))
    .otherwise(col("total_amount").cast("string"))
)

# ---- Write to S3 ----
# Security: Ensure the target S3 bucket has default encryption (SSE-S3 or SSE-KMS) enabled
print("Silver Layer: Writing to S3")
df.coalesce(10).write.mode("overwrite").parquet(silver_path)

# ---- Load to Redshift via COPY ----
# Security: The redshift_temp_dir bucket should have:
#   1. Server-side encryption enabled (SSE-S3 or SSE-KMS)
#   2. A lifecycle policy to auto-delete temp files (e.g., 1-day expiry)
#   3. Block Public Access enabled
print("Silver Layer: Loading to Redshift...")
dyf = DynamicFrame.fromDF(df, glueContext, "silver_data")
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=dyf,
    catalog_connection=args['redshift_conn_name'],
    connection_options={
        "dbtable": args['redshift_table'],
        "database": args['redshift_db'],
    },
    redshift_tmp_dir=args['redshift_temp_dir']
)
print("Silver Layer: Redshift load SUCCEEDED")

df.unpersist()
print(f"Silver Layer Complete: {initial_count} records written to S3 and Redshift")
spark.stop()

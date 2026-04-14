"""
Bronze Layer - Raw Data Ingestion
Generates 50K synthetic e-commerce records, writes to S3 as parquet.
This job succeeds without errors.
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, rand, floor, concat, when, array, element_at,
    date_add, monotonically_increasing_id, round as spark_round
)
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bronze_output_path'])
spark = SparkSession.builder.config("spark.sql.shuffle.partitions", "10").getOrCreate()
glueContext = GlueContext(spark.sparkContext)

# Security: Validate S3 path to prevent writing to unintended buckets
bronze_path = args['bronze_output_path']
assert bronze_path.startswith("s3://"), "bronze_output_path must be a valid S3 path"

num_records = 50_000

df_base = spark.range(0, num_records)
products = ["Laptop", "Headphones", "Keyboard", "Monitor", "Mouse", "Webcam", "Tablet", "Charger", "SSD", "RAM"]
regions = ["us-east", "us-west", "eu-west", "ap-south", "ap-northeast"]
statuses = ["completed", "pending", "shipped", "returned", "cancelled"]

df = (df_base
    .withColumn("order_id", concat(lit("ORD-"), col("id").cast("string")))
    .withColumn("customer_id", concat(lit("CUST-"), (floor(rand() * 50000)).cast("string")))
    .withColumn("product", element_at(array(*[lit(p) for p in products]), (floor(rand() * len(products)) + 1).cast("int")))
    .withColumn("quantity", (floor(rand() * 10) + 1).cast("int"))
    .withColumn("unit_price", spark_round(rand() * 500 + 10, 2))
    .withColumn("total_amount", spark_round(col("quantity") * col("unit_price"), 2))
    .withColumn("region", element_at(array(*[lit(r) for r in regions]), (floor(rand() * len(regions)) + 1).cast("int")))
    .withColumn("order_status", element_at(array(*[lit(s) for s in statuses]), (floor(rand() * len(statuses)) + 1).cast("int")))
    .withColumn("order_date", date_add(lit("2025-01-01"), (floor(rand() * 365)).cast("int")))
    .drop("id")
)

# Cost: Use coalesce instead of repartition to avoid full shuffle when reducing partitions
# Security: Ensure the target S3 bucket has default encryption (SSE-S3 or SSE-KMS) enabled
df.coalesce(10).write.mode("overwrite").parquet(bronze_path)
print(f"Bronze Layer Complete: {num_records} records written")
spark.stop()

"""
Gold Layer - Reads from Redshift, Aggregates, Writes back to Redshift

Uses the same Glue catalog connection that Silver uses (proven working).
Reads via from_jdbc_conf, writes via write_dynamic_frame.from_jdbc_conf.

FAILURE: total_amount is VARCHAR in Redshift (Silver injected "$" prefixed strings).
CAST to numeric produces NULLs. Writing to target with NOT NULL constraint fails.
"""
import re
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, when, month
)
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'redshift_conn_name', 'redshift_db',
    'redshift_source_table', 'redshift_target_table', 'redshift_temp_dir'
])

spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()
glueContext = GlueContext(spark.sparkContext)

# Security: Validate table names to prevent SQL injection in preactions
TABLE_NAME_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_.]+$')
source_table = args['redshift_source_table']
target_table = args['redshift_target_table']
assert TABLE_NAME_PATTERN.match(source_table), f"Invalid source table name: {source_table}"
assert TABLE_NAME_PATTERN.match(target_table), f"Invalid target table name: {target_table}"

# ---- Read from Redshift (same pattern as Silver write) ----
print("Gold Layer: Reading from Redshift...")

dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": args['redshift_conn_name'],
        "dbtable": source_table,
        "redshiftTmpDir": args['redshift_temp_dir'],
    }
)

df = dyf.toDF()
record_count = df.count()
print(f"Gold Layer: Read {record_count} records from Redshift")
df.printSchema()

# ---- Aggregation ----
print("Gold Layer: Casting total_amount to numeric for aggregation...")
df = df.withColumn("total_amount_numeric", col("total_amount").cast("double"))

null_count = df.filter(col("total_amount_numeric").isNull()).count()
print(f"Gold Layer: WARNING - {null_count} rows have non-numeric total_amount values")

print("Gold Layer: Running aggregation...")
df_agg = (df
    .withColumn("order_month", month(col("order_date")))
    .groupBy("region", "product", "order_month", "price_category")
    .agg(
        spark_sum("total_amount_numeric").alias("revenue_total"),
        count("order_id").alias("order_count"),
        avg("unit_price").alias("avg_unit_price"),
        spark_sum("quantity").alias("total_units_sold"),
    )
)

agg_count = df_agg.count()
print(f"Gold Layer: Aggregated to {agg_count} rows")

df_final = df_agg.withColumn(
    "revenue_per_order",
    col("revenue_total") / col("order_count")
)

# ---- Enrich with per-row detail for audit trail ----
# Join back to get individual order amounts for the detail table
df_detail = (df
    .withColumn("order_month", month(col("order_date")))
    .select(
        "region", "product", "order_month", "price_category",
        col("total_amount_numeric").alias("order_amount"),
        "order_id"
    )
)

# ---- Write to Redshift ----
# Target table has NOT NULL on order_amount
# NULL values from corrupted upstream data will cause COPY to fail
print("Gold Layer: Writing to Redshift...")
dyf_out = DynamicFrame.fromDF(df_detail, glueContext, "gold_output")
glueContext.write_dynamic_frame.from_options(
    frame=dyf_out,
    connection_type="redshift",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": args['redshift_conn_name'],
        "dbtable": target_table,
        "redshiftTmpDir": args['redshift_temp_dir'],
        "preactions": f"""
            DROP TABLE IF EXISTS {target_table};
            CREATE TABLE {target_table} (
                region VARCHAR(256),
                product VARCHAR(256),
                order_month INT,
                price_category VARCHAR(256),
                order_amount DOUBLE PRECISION NOT NULL,
                order_id VARCHAR(256)
            );
        """,
    }
)

print("Gold Layer Complete")
spark.stop()

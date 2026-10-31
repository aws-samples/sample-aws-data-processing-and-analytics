"""AWS Glue 5.1 PySpark ETL for batch-scoped writes to Amazon S3 Tables."""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

REQUIRED_ARGUMENTS = [
    "JOB_NAME",
    "batch_id",
    "raw_bucket",
    "source_prefix",
    "account_id",
    "table_bucket_name",
    "namespace",
    "table_name",
    "failure_scenario",
    "random_seed",
    "write_mode",
]
REQUIRED_SOURCE_FIELDS = {
    "order_id",
    "customer_id",
    "order_timestamp",
    "order_amount",
    "status",
    "batch_id",
}


def log(event: str, **details: object) -> None:
    print(
        json.dumps(
            {
                "event": event,
                "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                **details,
            },
            sort_keys=True,
            default=str,
        )
    )


def get_job_run_id(argv: list[str]) -> str:
    """Read Glue's built-in run ID without re-registering its CLI option."""

    try:
        value = argv[argv.index("--JOB_RUN_ID") + 1]
    except (ValueError, IndexError) as error:
        raise ValueError("AWS Glue did not supply --JOB_RUN_ID") from error
    if not value or value.startswith("--"):
        raise ValueError("AWS Glue supplied an invalid --JOB_RUN_ID value")
    return value


def validate_source_schema(frame: DataFrame) -> None:
    missing = sorted(REQUIRED_SOURCE_FIELDS - set(frame.columns))
    if missing:
        raise ValueError(f"Source schema is missing required fields: {missing}")


def apply_scenario(frame: DataFrame, scenario: str, seed: int) -> DataFrame:
    """Inject silent correctness defects without failing the Glue job."""

    if scenario == "healthy":
        return frame
    if scenario == "missing_records":
        return frame.filter(
            F.pmod(F.crc32(F.concat_ws(":", F.lit(seed), "order_id")), F.lit(10)) != 0
        )
    if scenario == "duplicate_records":
        duplicate = frame.filter(
            F.pmod(F.crc32(F.concat_ws(":", F.lit(seed), "order_id")), F.lit(20)) == 0
        )
        return frame.unionByName(duplicate)
    if scenario == "schema_drift":
        validate_source_schema(frame)
        return frame
    raise ValueError(f"Unsupported failure scenario: {scenario}")


def main() -> None:
    args = getResolvedOptions(sys.argv, REQUIRED_ARGUMENTS)
    job_run_id = get_job_run_id(sys.argv)
    batch_id = args["batch_id"]
    source_uri = (
        f"s3://{args['raw_bucket']}/{args['source_prefix'].strip('/')}/batch_id={batch_id}/"
    )
    spark_context = SparkContext.getOrCreate()
    spark_application_id = spark_context.applicationId

    spark = (
        SparkSession.builder.appName(args["JOB_NAME"])
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.defaultCatalog", "s3tables")
        .config("spark.sql.catalog.s3tables", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.s3tables.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"
        )
        .config(
            "spark.sql.catalog.s3tables.glue.id",
            f"{args['account_id']}:s3tablescatalog/{args['table_bucket_name']}",
        )
        .config(
            "spark.sql.catalog.s3tables.warehouse",
            f"s3://{args['table_bucket_name']}/warehouse/",
        )
        .getOrCreate()
    )
    glue_context = GlueContext(spark.sparkContext)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    log(
        "etl_started",
        batch_id=batch_id,
        pipeline_run_id=job_run_id,
        source_uri=source_uri,
        scenario=args["failure_scenario"],
        spark_application_id=spark_application_id,
    )
    source = spark.read.json(source_uri)
    validate_source_schema(source)
    scoped = source.filter(F.col("batch_id") == batch_id)
    transformed = apply_scenario(scoped, args["failure_scenario"], int(args["random_seed"]))
    curated = transformed.select(
        F.col("order_id").cast(T.StringType()),
        F.col("customer_id").cast(T.StringType()),
        F.to_timestamp("order_timestamp").alias("order_timestamp"),
        F.col("order_amount").cast(T.DecimalType(12, 2)).alias("order_amount"),
        F.col("status").cast(T.StringType()),
        F.col("batch_id").cast(T.StringType()),
        F.current_timestamp().alias("ingestion_timestamp"),
        F.input_file_name().alias("source_file"),
        F.current_timestamp().alias("processing_timestamp"),
        F.lit(job_run_id).alias("pipeline_run_id"),
    )

    target = f"s3tables.{args['namespace']}.{args['table_name']}"
    if args["write_mode"] != "replace_batch":
        raise ValueError("Only the idempotent replace_batch write mode is supported")

    # The table is partitioned by batch_id. overwritePartitions replaces only partitions
    # present in this DataFrame in one Iceberg commit, making same-batch retries idempotent.
    curated.writeTo(target).overwritePartitions()
    log(
        "etl_committed",
        batch_id=batch_id,
        pipeline_run_id=job_run_id,
        target=target,
        write_mode="replace_batch",
    )
    job.commit()


if __name__ == "__main__":
    main()

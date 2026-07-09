"""
Customer events daily rollup.

Reads the raw customer-events Parquet partition for the day, expands each event
record with derived per-event metrics, and writes a daily aggregate.

NOTE: This job has been failing in production with executor process crashes.
"""

import sys
from pyspark.sql import SparkSession

if len(sys.argv) < 2:
    print("Usage: customer_events_aggregator.py <s3-bucket-name>")
    sys.exit(1)

bucket = sys.argv[1]

spark = SparkSession.builder.appName("customer-events-aggregator").getOrCreate()

# Read raw events for the day
events = spark.read.parquet(f"s3://{bucket}/data/")


def expand_event(iterator):
    """Expand each event into derived per-event metrics for downstream rollup."""
    expanded = []
    for row in iterator:
        expanded.append(row.asDict())
        for _ in range(10):
            expanded.append(dict(row.asDict()))
    yield len(expanded)


# Compute daily totals
totals = events.repartition(1).rdd.mapPartitions(expand_event)
print(f"Total events processed: {totals.collect()}")

spark.stop()
sys.exit(0)

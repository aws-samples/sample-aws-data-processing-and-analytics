"""
Medallion Architecture DAG - Bronze → Silver → Gold
Orchestrates 3 Glue jobs sequentially. Triggered manually for blog demo.
"""
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

STACK_NAME = "blogagent-v6"

default_args = {
    "owner": "data-engineering",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="medallion_architecture_pipeline",
    default_args=default_args,
    description="Bronze → Silver → Gold Medallion Architecture with AWS Glue",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["medallion", "glue", "blog-demo"],
) as dag:

    bronze = GlueJobOperator(
        task_id="bronze_layer_ingestion",
        job_name=f"{STACK_NAME}-Bronze-Job",
        region_name="us-west-2",
        verbose=True,
        wait_for_completion=True,
        num_of_dpus=None,
    )

    silver = GlueJobOperator(
        task_id="silver_layer_transform",
        job_name=f"{STACK_NAME}-Silver-Job",
        region_name="us-west-2",
        verbose=True,
        wait_for_completion=True,
        num_of_dpus=None,
    )

    gold = GlueJobOperator(
        task_id="gold_layer_aggregate",
        job_name=f"{STACK_NAME}-Gold-Job",
        region_name="us-west-2",
        verbose=True,
        wait_for_completion=True,
        num_of_dpus=None,
    )

    bronze >> silver >> gold

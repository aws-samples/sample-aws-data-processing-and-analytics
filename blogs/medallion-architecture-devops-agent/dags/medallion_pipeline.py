"""
Medallion Architecture DAG - Bronze → Silver → Gold
Orchestrates 3 Glue jobs sequentially. Triggered manually for demo.
Stack name is auto-detected from the MWAA environment name.
"""
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

# Auto-detect stack name from MWAA environment (format: <stack-name>-mwaa)
# MWAA translates AirflowConfigurationOptions key "env_var.mwaa_env_name" to
# the environment variable AIRFLOW__ENV_VAR__MWAA_ENV_NAME on the Fargate container.
MWAA_ENV = os.environ.get("AIRFLOW__ENV_VAR__MWAA_ENV_NAME")
if not MWAA_ENV:
    raise RuntimeError("MWAA_ENV_NAME environment variable is not set")
STACK_NAME = MWAA_ENV.replace("-mwaa", "")

default_args = {
    "owner": "data-engineering",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="medallion_pipeline",
    default_args=default_args,
    description="Bronze → Silver → Gold Medallion Architecture with AWS Glue",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["medallion", "glue", "devops-agent-demo"],
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

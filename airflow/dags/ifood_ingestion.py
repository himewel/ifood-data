from datetime import datetime

import yaml
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "depends_on_past": True,
    "retries": 5,
    "wait_for_downstream": True,
}

with open("/opt/airflow/dags/config.yaml") as stream:
    config = yaml.safe_load(stream)

with DAG(
    dag_id="ifood-ingestion",
    default_args=default_args,
    description="Run automated data ingestion from S3 bucket",
    max_active_runs=1,
    schedule_interval="@yearly",
    start_date=datetime(2018, 1, 1),
    catchup=True,
) as dag:
    spark_conf = config["spark"]["conf"]
    spark_packages = config["spark"]["packages"]
    spark_total_executor_cores = config["spark"]["total_executor_cores"]

    for schema, params in config["data"].items():
        run_task = SparkSubmitOperator(
            application="/spark/src/extract_from_s3.py",
            application_args=[
                params["source"],
                schema,
                params["format"],
                ",".join(params["id_columns"]),
                params["partition_column"],
                "{{ data_interval_start }}",
                "{{ data_interval_end }}",
                "run",
            ],
            conf=spark_conf,
            total_executor_cores=2
            if schema == "orders"
            else spark_total_executor_cores,
            task_id=f"raw_{schema}",
            packages=",".join(spark_packages),
        )

        test_task = SparkSubmitOperator(
            application="/spark/src/extract_from_s3.py",
            application_args=[
                params["source"],
                schema,
                params["format"],
                ",".join(params["id_columns"]),
                params["partition_column"],
                "{{ data_interval_start }}",
                "{{ data_interval_end }}",
                "test",
            ],
            conf=spark_conf,
            total_executor_cores=2
            if schema == "orders"
            else spark_total_executor_cores,
            task_id=f"test_raw_{schema}",
            packages=",".join(spark_packages),
        )

        run_task >> test_task

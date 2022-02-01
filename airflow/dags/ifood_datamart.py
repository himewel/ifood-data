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
    dag_id="ifood-datamart",
    default_args=default_args,
    description="Run automated build of datamarts",
    max_active_runs=1,
    schedule_interval="@yearly",
    start_date=datetime(2018, 1, 1),
    catchup=True,
) as dag:
    spark_conf = config["spark"]["conf"]
    spark_packages = config["spark"]["packages"]
    spark_total_executor_cores = config["spark"]["total_executor_cores"]

    orders_task = SparkSubmitOperator(
        application="/spark/src/orders_datamart.py",
        application_args=[
            "{{ data_interval_start }}",
            "{{ data_interval_end }}",
        ],
        conf=spark_conf,
        total_executor_cores=spark_total_executor_cores,
        task_id="trusted_orders",
        packages=",".join(spark_packages),
    )

    orders_statuses_task = SparkSubmitOperator(
        application="/spark/src/order_statuses_datamart.py",
        application_args=[
            "{{ data_interval_start }}",
            "{{ data_interval_end }}",
        ],
        conf=spark_conf,
        total_executor_cores=spark_total_executor_cores,
        task_id="trusted_order_statuses",
        packages=",".join(spark_packages),
    )

    order_items_task = SparkSubmitOperator(
        application="/spark/src/order_items_datamart.py",
        application_args=[
            "{{ data_interval_start }}",
            "{{ data_interval_end }}",
        ],
        conf=spark_conf,
        total_executor_cores=spark_total_executor_cores,
        task_id="trusted_order_items",
        packages=",".join(spark_packages),
    )

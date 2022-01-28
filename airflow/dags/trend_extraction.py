from datetime import datetime

import pendulum
import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator

from include import TweepyExtractor, TweetIngestion, TweetRelease

default_args = {
    "depends_on_past": True,
    "retries": 5,
    "wait_for_downstream": True,
}

with open("dags/config.yaml") as stream:
    config = yaml.safe_load(stream)

for topic, values in config["topics"].items():
    start_date = pendulum.instance(values["start-date"])
    schedule_interval = values["schedule-interval"]
    max_results = values["max-results"]
    landing_path = values["landing-path"]
    raw_path = values["raw-path"]
    trusted_path = values["trusted-path"]

    with DAG(
        dag_id=f"{topic}-trending-extraction",
        default_args=default_args,
        description="Run automated data ingestion of tweets from trending topics",
        max_active_runs=1,
        schedule_interval=schedule_interval,
        start_date=start_date,
        tags=["Twitter", "Spark", "Tweepy", "DeltaLake"],
    ) as dag:
        extractor = TweepyExtractor(path=landing_path)
        extraction_task = PythonOperator(
            task_id="extract_from_api",
            python_callable=extractor.fetch,
            op_kwargs={"topic": f"#{topic}", "max_results": max_results},
        )

        ingestion = TweetIngestion(landing_path=landing_path, raw_path=raw_path)
        ingestion_task = PythonOperator(
            task_id="ingest_to_raw",
            python_callable=ingestion.evaluate,
        )

        release = TweetRelease(raw_path=raw_path, trusted_path=trusted_path)
        release_task = PythonOperator(
            task_id="release_to_trusted",
            python_callable=release.evaluate,
        )

        extraction_task >> ingestion_task >> release_task
        globals()[dag.dag_id] = dag

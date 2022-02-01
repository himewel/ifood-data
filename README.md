# Ifood Data

<p>
<img alt="Kubernetes" src="https://img.shields.io/badge/Kubernetes-%23326ce5.svg?&style=for-the-badge&logo=kubernetes&logoColor=white"/>
<img alt="Helm" src="https://img.shields.io/badge/Helm-%230F1689.svg?&style=for-the-badge&logo=helm&logoColor=white"/>
<img alt="Apache Airflow" src="https://img.shields.io/badge/apacheairflow-%23017cee.svg?&style=for-the-badge&logo=apache-airflow&logoColor=white"/>
<img alt="Apache Spark" src="https://img.shields.io/badge/apachespark-%23E25A1C.svg?&style=for-the-badge&logo=apachespark&logoColor=white"/>
<img alt="DeltaLake" src="https://img.shields.io/badge/delta-%23003366.svg?&style=for-the-badge&logo=delta&logoColor=white"/>
</p>

This project process semi-structured data and build a datalake that provides efficient storage and performance. The datalake is organized in the following 2 layers:
- *raw layer*: datasets must have the same schema as the source, but support fast structured data reading
- *trusted layer*: Datamarts as required by the analysis team


<p align="center">
<img alt="Data architecture" src="/docs/datalake.png"/>
</p>

The Datamarts required in trusted layer should be built as the following rules:
- *Order dataset*: one line per order with all data from order, consumer, restaurant and the LAST status from order statuses dataset. To help analysis, it would be a nice to have: data partitioned on the restaurant LOCAL date.
- *Order Items dataset*: easy to read dataset with one-to-many relationship with Order dataset. Must contain all data from order items column.
- *Order statuses*: Dataset containing one line per order with the timestamp for each
registered event: CONCLUDED, REGISTERED, CANCELLED, PLACED.

For the trusted layer, anonymize any sensitive data.

At the end of each ETL, use any appropriated methods to validate your data. Read
performance, watch out for small files and skewed data.

**Non functional requirements**

- Data volume increases each day.
- All ETLs must be built to be scalable.

## How to start

First of all, install the following softwares to reproduce this project:
- [kind](https://kind.sigs.k8s.io/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [helm](https://helm.sh/)
- [docker](https://www.docker.com/)

So, you can init the kubernetes environment with Airflow and Spark in the following commands:

```shell
make create-cluster
make create-namespace
make add-charts
make helm-init
```

To forward the Airflow Web UI to your browser, open another terminal and run:

```shell
make airflow-forward
```

To forward the Spark Web UI to your browser, open another terminal and run:

```shell
make spark-forward
```

To release the Airflow or Spark images of this project, run the release targets with a VERSION as parameter:

```shell
make airflow-release VERSION=0.0.1
make spark-release VERSION=0.0.1
```

To update the code files in the kubernetes environment, run the following:

```shell
make update
```

To wrap all these commands and start your first version, just run:

```shell
make all
```        

To clear your environment and remove the kubernetes cluster, run the following:

```shell
make clear
```

The AWS credentials are registered in the charts by environment variables, so to achieve it with Apache Airflow Chart complete the `env` section in `templates/airflow/airflow-chart.yaml` as the following example:

```yaml
env:
  - name: AWS_ACCESS_KEY_ID
    value: "MYACCESSKEYID1234"
  - name: AWS_SECRET_ACCESS_KEY
    value: "MY/SECRETACCESSKEY1234"
  - name: AIRFLOW_CONN_SPARK_DEFAULT
    value: "spark://spark%3A%2F%2Fifood-spark-master-svc:7077"
```

To make the same in the Apache Spark pods, create a section to master and workers in the `templates/spark/spark-chart.yaml` file as the following:

```yaml
master:
  extraEnvVars:
    - name: AWS_ACCESS_KEY_ID
      value: "MYACCESSKEYID1234"
    - name: AWS_SECRET_ACCESS_KEY
      value: "MY/SECRETACCESSKEY1234"
worker:
  extraEnvVars:
    - name: AWS_ACCESS_KEY_ID
      value: "MYACCESSKEYID1234"
    - name: AWS_SECRET_ACCESS_KEY
      value: "MY/SECRETACCESSKEY1234"
```

## Architecture

Both Apache Airflow and Apache Spark environments are deployed in Kubernetes using [Apache Airflow Official Helm Chart](https://airflow.apache.org/docs/helm-chart/stable/index.html) and [Apache Spark Bitnami Helm Chart](https://bitnami.com/stack/spark/helm). While running on local environment with kind, Apache Airflow architecture is deployed with only one worker and Apache Spark is deployed with three workers. A shared volume is created to sync code between the pods and the local folders.

Two DAGs are created to orchestrate the ETL jobs: *ifood-ingestion* and *ifood-datamart*. The DAG *ifood-ingestion* is responsible for the ingestion of `s3://ifood-data-architecture-source` files to the raw layer and make sure that each row was ingested to the data lake stored in `s3://ifood-lake/raw`. Then, the DAG *ifood-datamart* gets data from the raw layer and creates the specified datamarts. Both DAGs are scheduled to run yearly by default, besides each job could be configured to run with any schedule. Raw and trusted layers are writed with Delta Lake format in upsert mode.

The folder `airflow/` keeps the Apache Airflow Dockerfile, DAG files and other requirements to be deployed in Kubernetes. A similar pattern is used in the `spark/` folder, where the PySpark script are placed with a Dockerfile and their requirements. In `templates/` the Helm chart configs and other pod declarations are stored.

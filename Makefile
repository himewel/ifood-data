SHELL:=/bin/bash
CLUSTER_NAME=ifood-cluster
NAMESPACE=ifood-cluster
RELEASE_NAME=ifood

.PHONY: create-cluster
create-cluster:
	kind create cluster \
		--image kindest/node:v1.21.1 \
		--name ${CLUSTER_NAME}

.PHONY: create-namespace
create-namespace:
	kubectl create namespace ${NAMESPACE}

.PHONY: add-charts
add-charts:
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm repo add apache-airflow https://airflow.apache.org
	helm repo update

.PHONY: port-forward
port-forward:
	kubectl port-forward svc/${AIRFLOW_RELEASE_NAME}-webserver 8080:8080 \
		--namespace $NAMESPACE

.PHONY: helm-init
helm-init:
	helm install ${RELEASE_NAME}-airflow apache-airflow/airflow --namespace ${NAMESPACE}

.PHONY: airflow-release
airflow-release:
	docker build --tag ifood-airflow:${VERSION} airflow
	kind load docker-image ifood-airflow:${VERSION} --name ${CLUSTER_NAME}
	helm upgrade ${RELEASE_NAME}-airflow apache-airflow/airflow \
		--namespace ${NAMESPACE} \
		--values ./airflow/config.yaml

.PHONY: clear
clear:
	kind delete cluster --name ${CLUSTER_NAME}

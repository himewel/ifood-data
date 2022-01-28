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
	helm repo add nfs https://charts.helm.sh/stable
	helm repo update

.PHONY: airflow-forward
airflow-forward:
	kubectl port-forward svc/${RELEASE_NAME}-airflow-webserver 8080:8080 \
		--namespace ${NAMESPACE}

.PHONY: spark-forward
spark-forward:
	kubectl port-forward svc/${RELEASE_NAME}-spark-master-svc 8088:80 \
       --namespace ${NAMESPACE}

.PHONY: helm-init
helm-init:
	helm install ${RELEASE_NAME}-airflow apache-airflow/airflow \
		--namespace ${NAMESPACE}
	helm install ${RELEASE_NAME}-nfs nfs/nfs-server-provisioner \
  		--set persistence.enabled=true,persistence.size=5Gi \
		--namespace ${NAMESPACE}
	kubectl apply -f templates/nfs -n ${NAMESPACE}
	helm install ${RELEASE_NAME}-spark bitnami/spark \
		--values templates/spark/spark-chart.yaml \
		--namespace ${NAMESPACE}

.PHONY: airflow-release
airflow-release:
	docker build --tag ifood-airflow:${VERSION} airflow
	kind load docker-image ifood-airflow:${VERSION} --name ${CLUSTER_NAME}
	helm upgrade ${RELEASE_NAME}-airflow apache-airflow/airflow \
		--namespace ${NAMESPACE} \
		--values templates/airflow/airflow-chart.yaml \
		--set images.airflow.repository=ifood-airflow \
    	--set images.airflow.tag=${VERSION}

.PHONY: spark-release
spark-release:
	docker build --tag ifood-spark:${VERSION} spark
	kind load docker-image ifood-spark:${VERSION} --name ${CLUSTER_NAME}
	helm upgrade ${RELEASE_NAME}-spark bitnami/spark \
		--namespace ${NAMESPACE} \
		--values templates/spark/spark-chart.yaml \
		--set image.repository=ifood-spark \
    	--set image.tag=${VERSION}

.PHONY: update
update:
	kubectl exec \
		-it ${RELEASE_NAME}-airflow-worker-0 \
		--container worker \
		--namespace ${NAMESPACE} \
		-- rm -rf /opt/airflow/dags /spark/src
	kubectl cp airflow/dags ${RELEASE_NAME}-airflow-worker-0:/opt/airflow \
		--namespace ${NAMESPACE} \
		--container worker
	kubectl cp spark/src ${RELEASE_NAME}-airflow-worker-0:/spark \
		--namespace ${NAMESPACE} \
		--container worker

.PHONY: clear
clear:
	kind delete cluster --name ${CLUSTER_NAME}

FROM bitnami/spark:3.2.0-debian-10-r73

ENV PYTHONPATH "${SPARK_HOME}/python:${PYTHONPATH}:/spark/src"

USER root

COPY requirements.txt ./requirements.txt

RUN pip3 install --no-cache-dir --requirement ./requirements.txt

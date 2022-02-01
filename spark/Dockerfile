FROM bitnami/spark:3.2.0-debian-10-r73

ENV PYTHONPATH "${SPARK_HOME}/python:${PYTHONPATH}:/spark/src"

USER root

COPY requirements.txt ./requirements.txt
RUN pip3 install --no-cache-dir --requirement ./requirements.txt

COPY maven-packages.txt ./maven-packages.txt
RUN echo "exit()" >> ./spark_packages.py \
    && maven_packages=$(sed -z 's/\n/,/g;s/,$/\n/' ./maven-packages.txt) \
    && spark-submit \
        --packages $maven_packages \
        ./spark_packages.py \
    && rm -rf ./install_packages.py \
    && cp /root/.ivy2/jars/* ${SPARK_HOME}/jars

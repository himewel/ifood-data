FROM apache/airflow:2.2.3-python3.8

ENV JAVA_HOME "/usr/lib/jvm/java-8-openjdk-amd64"
ENV SPARK_HOME "/opt/spark"
ENV PATH "${PATH}:${JAVA_HOME}/bin"
ENV PATH "${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin"
ENV PYTHONPATH "${PYTHONPATH}:${AIRFLOW_HOME}"

USER root

RUN mkdir -p /usr/share/man/man1 \
    && (echo "deb http://security.debian.org/debian-security stretch/updates main" >> /etc/apt/sources.list) \
    && apt-get update \
    && apt-get install --no-install-recommends --yes \
        openjdk-8-jdk \
        libsasl2-dev \
        gcc \
        g++ \
    && apt-get clean \
    && rm -rf -- /var/lib/apt/lists/*

RUN curl -O https://ftp.unicamp.br/pub/apache/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz \
    && tar -xf spark-3.2.0-bin-hadoop3.2.tgz \
    && rm -rf spark-3.2.0-bin-hadoop3.2.tgz \
    && mv spark-3.2.0-bin-hadoop3.2 ${SPARK_HOME}

RUN usermod --uid 1000 airflow \
    && chown --recursive airflow ${AIRFLOW_HOME}

USER airflow

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir --requirement ./requirements.txt

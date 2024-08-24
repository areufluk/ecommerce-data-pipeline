# Base image
FROM apache/airflow:slim-2.7.3-python3.11

# Install openjdk
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Download spark drivers
RUN mkdir -p ${AIRFLOW_HOME}/jars
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -o ${AIRFLOW_HOME}/jars/postgresql-42.7.3.jar
RUN curl -L https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar -o ${AIRFLOW_HOME}/jars/gcs-connector-hadoop2-latest.jar

COPY ./requirements /tmp/requirements
RUN pip install --no-cache-dir -r /tmp/requirements/base.txt

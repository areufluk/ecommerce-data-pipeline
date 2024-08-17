# Use the official Bitnami Spark image as the base image
FROM bitnami/spark:3.5.2

# Set environment variables
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Install Python and necessary system dependencies
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install additional Python libraries
COPY ./requirements /tmp/requirements
RUN pip install --no-cache-dir -r /tmp/requirements/base.txt

# Download the PostgreSQL JDBC driver
RUN wget -P $SPARK_HOME/jars https://jdbc.postgresql.org/download/postgresql-42.2.18.jar

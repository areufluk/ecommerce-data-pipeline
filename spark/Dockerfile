# Use the official Bitnami Spark image as the base image
FROM bitnami/spark:3.5.2

# Set environment variables
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Install Python and necessary system dependencies
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

USER 1001

COPY ./requirements /tmp/requirements
RUN pip install --no-cache-dir -r /tmp/requirements/base.txt

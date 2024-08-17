FROM apache/airflow:slim-2.9.2-python3.11
USER airflow

COPY ./requirements /tmp/requirements
RUN pip install --no-cache-dir -r /tmp/requirements/base.txt

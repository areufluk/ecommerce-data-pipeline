from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from scripts.generate_order import create_or_update_order
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# Define .jar file path
postgres_jar = '/opt/airflow/jars/postgresql-42.7.3.jar'
gcs_connector_jar = '/opt/airflow/jars/gcs-connector-hadoop2-latest.jar'

jar_path = postgres_jar + ',' + gcs_connector_jar

default_args = {
    'owner': 'Chanayut',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    dag_id='sales_data_pipeline',
    description='A sales data pipeline',
    tags=['test'],
    schedule_interval=None,
    catchup=False,
    start_date=days_ago(1),
    default_args=default_args,
)

create_or_update_order_task = PythonOperator(
    task_id='first_task',
    python_callable=create_or_update_order,
    dag=dag
)

spark_job = SparkSubmitOperator(
    task_id='second_task',
    conn_id='spark_conn',
    application='/opt/airflow/scripts/extract_data.py',
    jars=jar_path,
    driver_class_path='/opt/airflow/jars/'
)

create_or_update_order_task >> spark_job
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from scripts.generate_order import create_or_update_order
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.param import Param
from datetime import timedelta, datetime


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
    params={
        'start_date_param': Param(f"{datetime.today().date() - timedelta(days=1)}", type="string", format="date"),
        'end_date_param': Param(f"{datetime.today().date() - timedelta(days=1)}", type="string", format="date")
    }
)

upsert_order_data_task = PythonOperator(
    task_id='upsert_order_data',
    python_callable=create_or_update_order,
    dag=dag,
    op_kwargs={
        'start_date': '{{params.start_date_param}}',
        'end_date': '{{params.end_date_param}}'
    }
)

extract_order_data_job = SparkSubmitOperator(
    task_id='extract_order_data',
    conn_id='spark_conn',
    application='/opt/airflow/scripts/extract_order_data.py',
    jars=jar_path,
    driver_class_path='/opt/airflow/jars/',
    dag=dag,
    application_args=['{{params.start_date_param}}', '{{params.end_date_param}}']
)

extract_customer_data_job = SparkSubmitOperator(
    task_id='extract_customer_data',
    conn_id='spark_conn',
    application='/opt/airflow/scripts/extract_customer_data.py',
    jars=jar_path,
    driver_class_path='/opt/airflow/jars/',
    dag=dag
)

extract_campaign_data_job = SparkSubmitOperator(
    task_id='extract_campaign_data',
    conn_id='spark_conn',
    application='/opt/airflow/scripts/extract_campaign_data.py',
    jars=jar_path,
    driver_class_path='/opt/airflow/jars/',
    dag=dag
)

extract_product_data_job = SparkSubmitOperator(
    task_id='extract_product_data',
    conn_id='spark_conn',
    application='/opt/airflow/scripts/extract_product_data.py',
    jars=jar_path,
    driver_class_path='/opt/airflow/jars/',
    dag=dag
)

transform_order_data_job = SparkSubmitOperator(
    task_id='transform_order_data',
    conn_id='spark_conn',
    application='/opt/airflow/scripts/transform_order_data.py',
    jars=jar_path,
    driver_class_path='/opt/airflow/jars/',
    dag=dag
)

transform_analytic_data_job = SparkSubmitOperator(
    task_id='transform_analytic_data',
    conn_id='spark_conn',
    application='/opt/airflow/scripts/transform_analytic_data.py',
    jars=jar_path,
    driver_class_path='/opt/airflow/jars/',
    dag=dag
)

(
    upsert_order_data_task
    >> extract_order_data_job
    >> [extract_customer_data_job, extract_campaign_data_job, extract_product_data_job]
    >> transform_order_data_job
    >> transform_analytic_data_job
)

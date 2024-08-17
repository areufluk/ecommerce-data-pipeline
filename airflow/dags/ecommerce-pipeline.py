from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from scripts.generate_order import create_or_update_order


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

create_or_update_order_task
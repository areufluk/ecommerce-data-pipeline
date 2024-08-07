from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


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

# task_a = PythonOperator(
#     task_id='first_task',
#     python_callable=trigger_with_name,
#     dag=trigger_dag,
#     op
# )
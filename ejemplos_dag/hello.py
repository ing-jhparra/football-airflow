from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

def print_hello():
    return 'Hello world from first Airflow DAG!'

dag = DAG(
        'hello', 
        description='Hello World DAG',
        schedule_interval=timedelta(days=1),
        start_date=days_ago(2), 
        catchup=False)
        
hello_operator = PythonOperator(
                 task_id='hello_task', 
                 python_callable=print_hello, 
                 dag=dag)

hello_operator
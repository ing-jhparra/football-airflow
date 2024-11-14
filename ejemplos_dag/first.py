from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

# Definir argumentos 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['jhparra@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Definir funciones a utilizar

def scrape ():
    logging.info('Realizando Scraping')

def process ():
    logging.info('Procesando datos crudos')

def save ():
    logging.info('Cargando y/o salvando datos')

# Definir el DAG
with DAG (
    'first',
    default_args = default_args,
    description = 'Un simple ejemplo',
    schedule_interval = timedelta(days=1),
    start_date = days_ago(2),
    tags = ['example']
) as dag :  # Definir las tareas a utilizar dentro d elas funciones
    scrape_task = PythonOperator(task_id='scrape', python_callable=scrape)
    process_task = PythonOperator(task_id='process', python_callable=process)
    save_task = PythonOperator(task_id='save', python_callable=save)

# Definir las dependencia entre tareas

scrape_task >> process_task >> save_task

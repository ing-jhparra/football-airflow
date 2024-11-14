from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Definimos los argumentos por defecto que se aplicarán a todas las tareas del DAG.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Creamos el DAG o Instanciamos un objeto DAG con los argumentos por defecto y una descripción
dag = DAG(
    'primer_flujo',
    default_args=default_args,
    description='Un DAG de ejemplo',
    schedule_interval=timedelta(days=1),
)


# Definamos lasTareas: DummyOperator
tarea_inicio = DummyOperator(
    task_id='inicio',
    dag=dag,
)

def inicio_con_saludo():
    print("Saludos desde mi primer flujo de trabajo")

tarea_python = PythonOperator(
    task_id='tarea_python',
    python_callable=inicio_con_saludo,
    dag=dag,
)

tarea_fin = DummyOperator(
    task_id='fin',
    dag=dag,
)

tarea_inicio >> tarea_python >> tarea_fin
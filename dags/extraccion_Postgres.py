from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook  # Usa el hook adecuado para tu base de datos
import pandas as pd
from datetime import datetime

# Función para extraer los datos y guardarlos en un CSV
def extract_to_csv():
    # Conexión a la base de datos
    pg_hook = PostgresHook(postgres_conn_id='my_database')  # Cambia 'my_database' al ID de tu conexión
    conn = pg_hook.get_conn()
    
    # Consulta SQL
    query = "SELECT * FROM mi_tabla"  # Cambia 'mi_tabla' al nombre de tu tabla
    
    # Extrae los datos usando pandas
    df = pd.read_sql(query, conn)
    
    # Guarda en un archivo CSV
    df.to_csv('/path/to/output_file.csv', index=False)  # Cambia la ruta al destino deseado

# Definición del DAG
with DAG(
    dag_id='extract_to_csv_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Tarea de extracción
    extract_task = PythonOperator(
        task_id='extract_to_csv_task',
        python_callable=extract_to_csv
    )
    
    extract_task

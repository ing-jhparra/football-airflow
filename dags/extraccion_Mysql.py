from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from pathlib import Path
from datetime import datetime
import pandas as pd

def extract_to_csv():
    # Conexión a la base de datos MySQL
    mysql_hook = MySqlHook(mysql_conn_id='my_mysql_database')  # Cambia 'my_mysql_database' al ID de tu conexión MySQL
    conn = mysql_hook.get_conn()
    
    # Fecha y hora actuales
    fecha_hora_actual = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    
    # Nombre del archivo
    archivo =  'reporte_' + str(fecha_hora_actual) + '.csv'
    
    # Ruta de destino
    # /var/lib/docker/overlay2/39299a7af3b928eff242a752b926353cad435d1523a92dbd71cb1e1b5b6f0258/merged/tmp/
    ruta = Path('/tmp/')  

    try:
        # Consulta SQL
        query = "SELECT title, description, release_year, rental_rate, LENGTH,replacement_cost,rating FROM film;"  # Cambia 'film' al nombre de tu tabla

        # Extrae los datos usando pandas
        df = pd.read_sql(query, conn)
        print(df.head(5))  # Muestra las primeras 10 filas para verificar
   
        # df.to_csv(str(ruta) + '/output_file.csv', index=False, sep=";")
        df.to_csv(ruta.joinpath(archivo), index=False, sep = ";")  # Cambia la ruta al destino deseado
        print(f'Datos guardados en {ruta}')

    finally:
        # Cierra la conexión
        conn.close()

# Definición del DAG
with DAG(
    dag_id='extract_mysql_to_csv_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Tarea de extracción
    extract_task = PythonOperator(
        task_id='extract_mysql_to_csv_task',
        python_callable=extract_to_csv
    )
    
    extract_task

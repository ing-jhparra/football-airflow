# Importamos
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task
import pandas as pd
from bs4 import BeautifulSoup
import requests
from lxml import html
from datetime import datetime

informacion = dict(provincia = list(),enlace = list(), ciudades = list())

encabezados = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
    }

#Definimos los argumentos por defecto que se aplicarán a todas las tareas del DAG.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#Creamos el DAG o Instanciamos un objeto DAG con los argumentos por defecto y una descripción
dag = DAG(
    'tercer_flujo',
    default_args=default_args,
    description='Un DAG de ejemplo',
    schedule_interval=timedelta(days=1),
)

# Definamos lasTareas: DummyOperator
tarea_inicio = DummyOperator(
    task_id='inicio',
    dag=dag,
)

def scraping():

    url = 'https://colegiosargentina.com.ar/'

    respuesta_obtenida = requests.get(url,headers=encabezados)

    if respuesta_obtenida.ok:
        print('Se obtuvo respuesta exitosa')
        html_obtenido = respuesta_obtenida.text
        soup = BeautifulSoup(html_obtenido,'html.parser')
        provincias_html = soup.find_all('span',{'class':'su-custom-gallery-title'})
        provincias = [p.text.strip() for p in provincias_html]
        enlaces_provincias = soup.find_all('div',{'class':'su-custom-gallery-slide'})
        enlaces = ['https://colegiosargentina.com.ar' + str(e.find('a').get('href')) for e in enlaces_provincias]
        informacion['provincia'] = provincias
    informacion['enlace'] = enlaces

    info_ciudades = list()

    for ind,url in enumerate(informacion['enlace']):
        print(f'Extrayendo ciudades de : {informacion["provincia"][ind]}')
        respuesta_obtenida = requests.get(url, headers=encabezados)
        if respuesta_obtenida.ok:
            html_obtenido = respuesta_obtenida.text
            soup = BeautifulSoup(html_obtenido,'html.parser')
            ciudades_html = soup.find_all('a',{'class':'localidad-button'})
            ciudades = [c.text for c in ciudades_html]
            enlaces = [c.get('href') for c in ciudades_html]
            info = [{'nombre_ciudad':ciudades[i], 'url':enlaces[i]} for i in range(len(ciudades))]
            info_ciudades.append(info)
    informacion['ciudades'] = info_ciudades

@task()
def procesar_datos():    
    print("Entro !!!")
    
tarea_python_2 = PythonOperator(
    task_id='tarea_python_2',
    python_callable=procesar_datos,
    dag=dag,
)
    
tarea_python = PythonOperator(
    task_id='tarea_python',
    python_callable=scraping,
    dag=dag,
)

tarea_fin = DummyOperator(
    task_id='fin',
    dag=dag,
)

tarea_inicio >> tarea_python >> tarea_python_2 >> tarea_fin
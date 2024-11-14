# 1. La librería principal que contiene todas las funcionalidades de Airflow
from airflow import DAG
# 2. Dependiendo de las tareas que necesites, puedes importar diferentes operadores. Algunos de los más comunes son
# Para tareas de prueba o marcadores de posición
from airflow.operators.dummy_operator import DummyOperator
# Para ejecutar funciones de Python
from airflow.operators.python_operator import PythonOperator
# Para ejecutar comandos de Bash.
from airflow.operators.bash_operator import BashOperator

# 3. Para manejar fechas y horas, especialmente para definir la fecha de inicio y el intervalo de programación del DAG.
from datetime import datetime, timedelta
# 4. Para facilitar el manejo de fechas, como days_ago
from airflow.utils.dates import days_ago


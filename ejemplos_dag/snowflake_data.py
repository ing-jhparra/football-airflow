from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


@dag(start_date=datetime(2024, 1, 1), 
     schedule='@daily', 
     catchup=False,
     default_args={"owner": "Astro", "retries": 1},
     tags = ['snowflake','LIBROS'] 
     )
def snowflake_data():
    # Primera tarea: consulta de datos en Snowflake
    @task
    def query_snowflake_data():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        
        # Consulta SQL en Snowflake
        query_sql = """
            SELECT
              COUNT(*) AS row_count,
              SUM(PRECIO_USD) AS sum_price
            FROM LIBRO
        """
        
        # Ejecutar la consulta
        result = hook.get_pandas_df(query_sql)
        return result

    # Segunda tarea: guardar el estatus de lectura en otra tabla
    @task
    def update_status_in_table():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

        # SQL para insertar el estatus de lectura
        insert_sql = """
            INSERT INTO STATUS (table_name, status, timestamp)
            VALUES ('LIBRO', 'COMPLETEDO', CURRENT_TIMESTAMP)
        """

        # Ejecutar la consulta
        hook.run(insert_sql)

    # Definir el orden de las tareas
    data = query_snowflake_data()
    
    data >> update_status_in_table()


snowflake_data()

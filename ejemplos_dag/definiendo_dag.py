from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(default_args=default_args, 
     schedule_interval=None, 
     start_date=days_ago(2))
def flujo_tarea():
    @task
    def extract():
        return {"1001": 301.27, "1002": 433.21, "1003": 502.22}

    @task
    def transform(order_data_dict: dict) -> dict:
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}
    
    @task()
    def load(total_order_value: float):
        print("Total order value is: %.2f" % total_order_value)

        order_data = extract()
        order_summary = transform(order_data)
        load(order_summary["total_order_value"])
        
    tutorial_dag = flujo_tarea()
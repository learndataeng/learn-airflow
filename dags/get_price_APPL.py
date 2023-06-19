from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(dag_id="get_price_APPL",
    start_date=datetime(2023, 6, 15),
    schedule='@daily',
    catchup=True) as dag:

    @task
    def extract(symbol):
        return symbol

    @task
    def process(symbol):
        return symbol

    @task
    def store(symbol):
        return symbol

    store(process(extract("APPL")))
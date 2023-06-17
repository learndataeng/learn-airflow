from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
from datetime import timedelta
# from plugins import slack

import requests
import logging
import psycopg2



def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


def extract(**context):
    link = context["params"]["url"]
    task_instance = context['task_instance']
    execution_date = context['execution_date']

    logging.info(execution_date)
    f = requests.get(link)
    return (f.text)


def transform(**context):
    logging.info("Transform started")    
    text = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    lines = text.strip().split("\n")[1:] # 첫 번째 라인을 제외하고 처리
    records = []
    for l in lines:
      (name, gender) = l.split(",") # l = "Keeyong,M" -> [ 'keeyong', 'M' ]
      records.append([name, gender])
    logging.info("Transform ended")
    return records


def load(**context):
    logging.info("load started")    
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    
    records = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")    
    """
    records = [
      [ "Keeyong", "M" ],
      [ "Claire", "F" ],
      ...
    ]
    """
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.name_gender;") 
        # DELETE FROM을 먼저 수행 -> FULL REFRESH을 하는 형태
        for r in records:
            name = r[0]
            gender = r[1]
            print(name, "-", gender)
            sql = f"INSERT INTO {schema}.name_gender VALUES ('{name}', '{gender}')"
            cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;") 
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    logging.info("load done")


dag = DAG(
    dag_id = 'name_gender_v4',
    start_date = datetime(2023,4,6), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 2 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
)


extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    params = {
        'url':  Variable.get("csv_url")
    },
    dag = dag)

transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    params = { 
    },  
    dag = dag)

load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    params = {
        'schema': 'keeyong',   ## 자신의 스키마로 변경
        'table': 'name_gender'
    },
    dag = dag)

extract >> transform >> load

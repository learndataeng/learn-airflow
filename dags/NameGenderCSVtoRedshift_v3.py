from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta
import requests
import logging
import psycopg2

def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "keeyong"  # 본인 ID 사용
    redshift_pass = "..."  # 본인 Password 사용
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect(f"dbname={dbname} user={redshift_user} host={host} password={redshift_pass} port={port}")
    conn.set_session(autocommit=True)
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

    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
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
    logging.info("load done")


dag = DAG(
    dag_id = 'name_gender_v3',
    start_date = datetime(2023,4,6), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 2 * * *',  # 적당히 조절
    catchup = False,
    max_active_runs = 1,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
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
        'schema': 'keeyong',
        'table': 'name_gender'
    },
    dag = dag)

extract >> transform >> load

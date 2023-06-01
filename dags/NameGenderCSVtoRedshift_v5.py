from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

import requests
import logging


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def extract(url):
    logging.info(datetime.utcnow())
    f = requests.get(url)
    return f.text


@task
def transform(text):
    lines = text.strip().split("\n")[1:] # 첫 번째 라인을 제외하고 처리
    records = []
    for l in lines:
      (name, gender) = l.split(",") # l = "Keeyong,M" -> [ 'keeyong', 'M' ]
      records.append([name, gender])
    logging.info("Transform ended")
    return records


@task
def load(schema, table, records):
    logging.info("load started")    
    cur = get_Redshift_connection()   
    """
    records = [
      [ "Keeyong", "M" ],
      [ "Claire", "F" ],
      ...
    ]
    """
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
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


with DAG(
    dag_id='namegender_v5',
    start_date=datetime(2022, 10, 6),  # 날짜가 미래인 경우 실행이 안됨
    schedule='0 2 * * *',  # 적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:

    url = Variable.get("csv_url")
    schema = 'keeyong'   ## 자신의 스키마로 변경
    table = 'name_gender'

    lines = transform(extract(url))
    load(schema, table, lines)

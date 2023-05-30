from airflow import DAG
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta
# from plugins import slack

import requests
import logging


def get_Redshift_connection(autocommit=False):
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
    lines = text.split("\n")[1:]
    return lines


@task
def load(schema, table, lines):
    cur = get_Redshift_connection()
    sql = f"BEGIN; DELETE FROM {schema}.{table};"
    for line in lines:
        if line != "":
            (name, gender) = line.split(",")
            logging.info(f"{name} - {gender}")
            sql += f"""INSERT INTO {schema}.{table} VALUES ('{name}', '{gender}');"""
    sql += "END;"
    logging.info(sql)
    cur.execute(sql)


with DAG(
    dag_id='NameGender_v5',
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

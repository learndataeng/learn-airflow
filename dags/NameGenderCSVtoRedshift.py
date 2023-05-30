from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import logging
import psycopg2

def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    user = "keeyong"  # 본인 ID 사용
    password = "..."  # 본인 Password 사용
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect(f"dbname={dbname} user={user} host={host} password={password} port={port}")
    conn.set_session(autocommit=True)
    return conn.cursor()


def extract(url):
    logging.info("Extract started")
    f = requests.get(url)
    logging.info("Extract done")
    return (f.text)


def transform(text):
    logging.info("Transform started")	
    lines = text.strip().split("\n")[1:] # 첫 번째 라인을 제외하고 처리
    records = []
    for l in lines:
      (name, gender) = l.split(",") # l = "Keeyong,M" -> [ 'keeyong', 'M' ]
      records.append([name, gender])
    logging.info("Transform ended")
    return records


def load(records):
    logging.info("load started")
    """
    records = [
      [ "Keeyong", "M" ],
      [ "Claire", "F" ],
      ...
    ]
    """
    schema = "keeyong"
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


def etl():
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    data = extract(link)
    lines = transform(data)
    load(lines)


dag_second_assignment = DAG(
	dag_id = 'name_gender',
	catchup = False,
	start_date = datetime(2023,4,6), # 날짜가 미래인 경우 실행이 안됨
	schedule = '0 2 * * *')  # 적당히 조절

task = PythonOperator(
	task_id = 'perform_etl',
	python_callable = etl,
	dag = dag_second_assignment)

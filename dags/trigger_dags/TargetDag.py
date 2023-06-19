from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    'TargetDag',
    schedule='@once',  # 매일 실행
    start_date=datetime(2023, 6, 1),
)

task1 = BashOperator(
    task_id='task1',
    bash_command="""echo '{{ ds }}, {{ dag_run.conf.get("path", "none") }}' """,
    dag=dag
)

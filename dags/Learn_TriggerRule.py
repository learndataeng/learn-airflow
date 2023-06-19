from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2023, 6, 15)
}

with DAG("Learn_TriggerRule", default_args=default_args, schedule=timedelta(1)) as dag:
   t1 = BashOperator(task_id="print_date", bash_command="date")
   t2 = BashOperator(task_id="sleep", bash_command="sleep 5")
   t3 = BashOperator(task_id="exit", bash_command="exit 1")
   t4 = BashOperator(
       task_id='final_task',
       bash_command='echo DONE!',
       trigger_rule=TriggerRule.ALL_DONE
   )
   [t1, t2, t3] >> t4

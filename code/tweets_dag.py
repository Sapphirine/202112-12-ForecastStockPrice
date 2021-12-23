from datetime import datetime, timedelta
from textwrap import dedent
import time

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'carson',
    'depends_on_past': False,
    'email': ['hs3228@columbia.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'start_date': datetime(2021, 11, 22),
    # 'end_date': datetime(2021, 11, 23),
    # 'schedule_interval': timedelta(minutes=30)
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(
    'tweet_dag',
    default_args=default_args,
    description='DAG of realtime tweet',
    schedule_interval="0 23 * * *",
    start_date=datetime(2021, 12, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # t* examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='fetch_realtime_tweets',
        bash_command='python /home/carsonsow/pj/code/twitter_stream.py',
        retries=3
    )
    
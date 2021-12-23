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
    'prediction_dag',
    default_args=default_args,
    description='DAG of stock prediction',
    schedule_interval="0 23 * * *",
    start_date=datetime(2021, 12, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # t* examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='fetch_daily_stock',
        bash_command='python /home/carsonsow/pj/code/fetch_finance.py',
        retries=3
    )

    t2 = BashOperator(
        task_id='analyze_daily_tweet_sents',
        bash_command='python /home/carsonsow/pj/code/analyze_sent.py',
        retries=3
    )

    t3 = BashOperator(
        task_id='process_tweets_sents',
        bash_command='python /home/carsonsow/pj/code/process_tweet_sent.py',
        retries=3
    )

    t4 = BashOperator(
        task_id='make_daily_data',
        bash_command='python /home/carsonsow/pj/code/make_daily_data.py',
        retries=3
    )

    t5 = BashOperator(
        task_id='predict_tomorrow',
        bash_command='python /home/carsonsow/pj/code/predict.py',
        retries=3
    )

    t2 >> t3
    [t1, t3] >> t4
    t4 >> t5
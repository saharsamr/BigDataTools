from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    'news',
    default_args={
        'depends_on_past': False,
        'retries': 2,
    },
    description='crawling news data',
    schedule_interval=timedelta(hours=3),
    start_date=datetime(2022, 6, 23, 7, 30, 00),
    catchup=False,
    tags=['news'],
) as dag:

    t0 = BashOperator(
        task_id='installation',
        bash_command='apt update && apt install wget && pip3 install bs4 jdatetime pandas'
    )
    t1 = BashOperator(
        task_id='crawl-data',
        bash_command="""
            wget --tries 1 —user-agent “BigDataClass” --recursive -q --level 1  --accept-regex '/news/[0-9]+/\.*' https://www.tgju.org/news/ \
            -P /opt/airflow/logs/ || true
        """,
    )
    t2 = BashOperator(
        task_id='extrac-from-webpages',
        bash_command='python3 /opt/airflow/codes/tgju_parser.py'
    )
    t3 = BashOperator(
        task_id='remove-temp-files',
        bash_command='rm -r /opt/airflow/logs/www.tgju.org/ || true'
    )

    t0 >> t1 >> t2 >> t3



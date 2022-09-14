from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    'merge',
    default_args={
        'depends_on_past': False,
        'retries': 0,
    },
    description='merging day data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 23, 9, 50, 00),
    catchup=False,
    tags=['merge'],
) as dag:

    t0 = BashOperator(
        task_id='start',
        bash_command='pip3 install kafka-python requests jdatetime'
    )

    t1 = BashOperator(
        task_id='merge-crypto',
        # bash_command='python3 /opt/airflow/codes/crypto_merge.py | xargs rm || true'
        bash_command='python3 /opt/airflow/codes/crypto_merge.py'
    )
    t2 = BashOperator(
        task_id='crypto-put-to-hdfs',
        bash_command='hdfs dfs -mkdir -p hdfs://namenode:9000/crypto/year={{ macros.datetime.now().year }}/month={{ macros.datetime.now().month }} || true\
        && hdfs dfs -put $(ls -Art /opt/airflow/logs/*tsv | tail -n 1) \
        hdfs://namenode:9000/crypto/year={{ macros.datetime.now().year }}/month={{ macros.datetime.now().month }}/ || true'
    )
    t3 = BashOperator(
        task_id='crypto-send-to-producer',
        bash_command='python3 /opt/airflow/codes/produce_from_hdfs.py crypto $(ls -Art /opt/airflow/logs/*tsv | tail -n 1)'
    )

    t4 = BashOperator(
        task_id='merge-news',
        # bash_command='python3 /opt/airflow/codes/news_merge.py | xargs rm || true'
        bash_command='python3 /opt/airflow/codes/news_merge.py'
    )
    t5 = BashOperator(
        task_id='remove-redundant-data',
        bash_command='python3 /opt/airflow/codes/news_remove_redundancy.py $(ls -Art /opt/airflow/logs/*csv | tail -n 1)'
    )
    t6 = BashOperator(
        task_id='news-put-to-hdfs',
        bash_command='hdfs dfs -mkdir -p hdfs://namenode:9000/news/year={{ macros.datetime.now().year }}/month={{ macros.datetime.now().month }} || true\
            && hdfs dfs -put $(ls -Art /opt/airflow/logs/*csv | tail -n 1) \
            hdfs://namenode:9000/news/year={{ macros.datetime.now().year }}/month={{ macros.datetime.now().month }}/ || true'
    )
    t7 = BashOperator(
        task_id='news-send-to-producer',
        bash_command='python3 /opt/airflow/codes/produce_from_hdfs.py news $(ls -Art /opt/airflow/logs/*csv | tail -n 1)'
    )

    t0 >> t1 >> t2 >> t3
    t0 >> t4 >> t5 >> t6 >> t7

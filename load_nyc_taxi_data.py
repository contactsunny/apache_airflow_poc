
from datetime import timedelta
from textwrap import dedent

from airflow import DAG
import os

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'sunny',
    'depends_on_past': False,
    'email': ['sunny@contactsunny.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'load_nyc_taxi_data',
    default_args=default_args,
    description='DAG to load NYC Taxi data to Hive',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['sunny', 'sample'],
) as dag:

    start = DummyOperator(
        task_id='start',
        dag=dag
    )

    create_database = BashOperator(
        task_id='create_database',
        bash_command='hive -f /mnt/d/code/poc/airflow/create_database.hql',
        dag=dag
    )

    create_table = BashOperator(
        task_id='create_table',
        bash_command='hive -f /mnt/d/code/poc/airflow/create_table.hql',
        dag=dag
    )

    download_nyc_yellow_taxi_data = BashOperator(
        task_id='download_nyc_yellow_taxi_data',
        bash_command='/mnt/d/code/poc/airflow/download_dataset.sh ',
        dag=dag
    )

    load_data_to_table = BashOperator(
        task_id='load_data_to_table',
        bash_command='hive -f /mnt/d/code/poc/airflow/load_data_to_table.hql',
        dag=dag
    )

    end = DummyOperator(
        task_id='end',
        dag=dag
    )

    start >> create_database >> create_table >> download_nyc_yellow_taxi_data >> load_data_to_table >> end

    
import os
PROJECT_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../")
# print(PROJECT_ROOT)

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": 'airflow',
    "depends_on_past": False,
    "start_date": datetime(2023, 11, 20),
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="twitter_daily_dag",
    default_args=default_args,
    catchup=False,
    schedule="0 0 * * *"
) as dag:
    
    start_task = EmptyOperator(task_id="twitter_crawler_start")

    producer_1_task = BashOperator(
        task_id="producer_1_task",
        bash_command=f"cd {PROJECT_ROOT}/kafka && python3 producer.py --id 1 --airflow",
        retries=2,
        max_active_tis_per_dag=1
    )

    producer_2_task = BashOperator(task_id="producer_2_task",
        bash_command=f"cd {PROJECT_ROOT}/kafka && python3 producer.py --id 2 --airflow",
        retries=2,
        max_active_tis_per_dag=1
    )
    
    producer_3_task = BashOperator(task_id="producer_3_task",
        bash_command=f"cd {PROJECT_ROOT}/kafka && python3 producer.py --id 3 --airflow",
        retries=2,
        max_active_tis_per_dag=1
    )

    # Integrated into Spark Streaming
    consumer_task = EmptyOperator(task_id="consumer_task")
    # consumer_task = BashOperator(task_id="consumer_task",
    #     bash_command=f"cd {PROJECT_ROOT}/kafka && python3 consumer.py",
    #     retries=1,
    #     max_active_tis_per_dag=1
    # )

    end_task = EmptyOperator(task_id="twitter_crawler_end")
    start_task >> (producer_1_task, producer_2_task, producer_3_task) >> consumer_task >> end_task
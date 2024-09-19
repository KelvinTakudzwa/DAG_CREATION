from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Takudzwa',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='my_first_dag_v3',
    default_args=default_args,
    description='This is my first DAG',
    start_date=datetime(2024, 9, 20, 3),
    schedule_interval='@daily',
) as dag:

    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hey, hello world, this is the first task!"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo hey, warrup warrup I'm the second task"
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command="echo hey, I'm the third task"
    )

    task4 = BashOperator(
        task_id='fourth_task',
        bash_command="echo hey, I'm the fourth task"
    )

    # Simplified dependencies
    task1 >> [task2, task3, task4]

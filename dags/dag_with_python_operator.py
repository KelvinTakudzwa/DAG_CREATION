from airflow import DAG
from datetime import datetime,timedelta
from  airflow.operators.python import PythonOperator



def greet(name,age):
    #print("hello world!")
    print(f"hello world my name is {name} and I am {age} years old")
    
def get_name():
    return Jerry

default_args={
    'owner':'Takudzwa',
    'retries':5,
    'retry_delay':timedelta(minutes=5),
}


with DAG(
    default_args=default_args,
    dag_id='my_first_dag_with_python_operator_v03',
    description='Our first dag using python operator',
    start_date=datetime(2024,9,20),
    schedule_interval='@daily',
)as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'name':'Takudzwa','age':23}
    ) 
    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name,
        
    )
    task2
        
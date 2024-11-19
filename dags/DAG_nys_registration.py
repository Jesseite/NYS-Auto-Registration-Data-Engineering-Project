from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import script.insert_mongo as insert_mongo
import script.transform_data as transform_data
import script.load_postgres as load_postgres

def insert_data():
    insert_mongo.main()

def transform_data_func():
    transform_data.main()

def load_data():
    load_postgres.main()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 17),
    'retries': 1
}

dag = DAG(
    'data_pipeline',
    default_args=default_args,
    schedule_interval='@daily'
)

t1 = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    dag=dag
)

t2 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data_func,
    dag=dag
)

t3 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

t1 >> t2 >> t3

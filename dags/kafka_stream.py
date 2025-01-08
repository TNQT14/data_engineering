from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'airschoolar',
    'start_date': datetime(2025,1,1,10,10)
}
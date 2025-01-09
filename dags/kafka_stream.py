from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'airschoolar',
    'start_date': datetime(2025,1,1,10,10)
}

def stream_data():
    import json
    import requests

    res = requests.get('https://randomuser.me/api/')
    data = res.json()
    # Pretty-print the JSON
    print(data)
    print(json.dumps(data, indent=0))

# with DAG('user_automation',
#          default_args = default_args,
#          schedule_interval= '@daily',
#          catchup= False) as dag:
#
#     streaming_task = PythonOperator(
#         task_id= 'stream_data_from_api',
#         python_callable=stream_data
#     )

stream_data();
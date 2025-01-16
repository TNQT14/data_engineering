import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'airschoolar',
    'start_date': datetime(2025,1,1,10,10)
}

i = 0

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 5:  # 1 minute
            break
        try:
            res = get_data()
            res = format_data(res, curr_time)

            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

with DAG('user_automation',
         default_args = default_args,
         schedule_interval= '*/10 * * * * *',
         catchup= False) as dag:

    streaming_task = PythonOperator(
        task_id= 'stream_data_from_api',
        python_callable=stream_data
    )

def get_data():
    import requests

    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    return res

def format_data(res, curr_time):
    import time
    import datetime

    data = {}
    location = res['location']
    # data['id'] = uuid.uuid4()
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    # Convert to a datetime object
    converted_time = datetime.datetime.fromtimestamp(time.time())

    # Format the datetime object to a readable string
    formatted_time = converted_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    data['created_time'] = formatted_time
    data['curr_time'] = datetime.datetime.fromtimestamp(curr_time).strftime("%Y-%m-%d %H:%M:%S.%f")

    return data

# stream_data()

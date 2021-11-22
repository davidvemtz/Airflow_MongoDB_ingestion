import json
from datetime import datetime
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook


default_args = {
    'start_date': datetime(2021, 1, 1)
}


def _processing_user(ti):
    users = ti.xcom_pull(task_ids=['extracting_user'])
    if not len(users) or 'results' not in users[0]:
        raise ValueError('User is empty')
    user = users[0]['results'][0]
    processed_user = dict({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email'],
        'age': user["dob"]['age']
    })
    ti.xcom_push(key='user', value=processed_user)


def _insert_to_mongo(ti):
    import pymongo
    user = ti.xcom_pull(key='user', task_ids='processing_user')
    uri = "mongodb://admin:admin@mongo:27017/deb-test?authSource=admin"
    client = pymongo.MongoClient(uri)
    print(client['deb-test'])
    database = client['deb-test']
    collection = database['randomusers']
    juser = json.loads(str(user).replace('\'', '"'))
    x = collection.insert_one(juser)
    return str(x)


def _count_doc(ti):
    import pymongo
    uri = "mongodb://admin:admin@mongo:27017/deb-test?authSource=admin"
    client = pymongo.MongoClient(uri)
    print(client['deb-test'])
    database = client['deb-test']
    collection = database['randomusers']
    x = collection.count()
    return str(x)


with DAG('user_processing_mongo', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/'
    )

    extracting_user = SimpleHttpOperator(
        task_id='extracting_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    processing_user = PythonOperator(
        task_id='processing_user',
        python_callable=_processing_user
    )

    storing_user = PythonOperator(
        task_id='storing_user',
        python_callable=_insert_to_mongo,
        provide_context=True
    )

    count_doc = PythonOperator(
        task_id='count_doc',
        python_callable=_count_doc,
        provide_context=True
    )

    is_api_available >> extracting_user >> processing_user >> storing_user >> count_doc
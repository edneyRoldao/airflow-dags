from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import requests
import json


def get_new_york_city_data():
    url = 'https://data.cityofnewyork.us/resource/rc75-m7u3.json'
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content))
    qtd = len(df.index)
    return qtd


def is_valid(task_instance):
    qtd = task_instance.xcom_pull(task_ids='get_ny_data')
    if qtd > 1000:
        return 'valid'
    return 'not_valid'


with DAG('tutorial_dag', start_date=datetime(2022, 8, 3), schedule_interval='30 * * * *', catchup=False) as dag:

    get_ny_data = PythonOperator(
        task_id='get_ny_data',
        python_callable=get_new_york_city_data
    )

    is_valid = PythonOperator(
        task_id='is_valid',
        python_callable=is_valid
    )

    valid = BashOperator(
        task_id='valid',
        bash_command="echo 'quantity is OK'"
    )

    not_valid = BashOperator(
        task_id='not_valid',
        bash_command="echo 'quantity is NOT OK'"
    )

    get_ny_data >> is_valid >> [valid, not_valid]

import datetime as dt
import requests
import pandas as pd
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'mikhail',
    'start_date': dt.datetime(2021, 1, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

def data_analyst_info():
    vacancies = []
    vacancy_query = "data analyst"
    for page_number in range(50):
        response = requests.get("https://api.hh.ru/vacancies",
                           params = {
                               "text": vacancy_query,
                               "page": page_number
    })
        response_json=response.json()
        vacancies+=response_json['items']

    vacancies_dataset = {
        "vacancy_name": [],
        'employer':[],
        "city": []
    }

    for vacancy in vacancies:
        vacancies_dataset["vacancy_name"].append(vacancy["name"])
        vacancies_dataset["employer"].append(vacancy["employer"]['name'])
        vacancies_dataset["city"].append(vacancy["area"]["name"])
    vacancies_dataset = pd.DataFrame(vacancies_dataset)
    df=vacancies_dataset['city'].value_counts()
    df.to_csv('data_analyst_city_count.csv')
    vacancies_dataset.to_csv('data_analyst.csv')
    print("Files saved Successfully")

with DAG(dag_id='data_analyst_info', default_args=args, schedule_interval=None) as dag:
    data_analyst_info = PythonOperator(
        task_id='data_from_hh_api',
        python_callable=data_analyst_info,
        dag=dag
    )
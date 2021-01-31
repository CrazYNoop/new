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

def download_data_from_hh():
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
    "city": []
}

    for vacancy in vacancies:
        vacancies_dataset["vacancy_name"].append(vacancy["name"])
        vacancies_dataset["city"].append(vacancy["area"]["name"])
    vacancies_dataset = pd.DataFrame(vacancies_dataset)
    df=vacancies_dataset['city'].value_counts()    
    df.to_csv(os.path.join(os.path.expanduser('~'), 'data_analyst.csv'))

with DAG(dag_id='data_analyst_info', default_args=args, schedule_interval=@daily) as dag:
    take_data_hh = PythonOperator(
        task_id='download_data_from_hh',
        python_callable=download_data_from_hh,
        dag=dag
    )
    say_hello = PythonOperator(
        task_id='say_hello',
        python_callable="print('Hello airflow')",
        dag=dag
    )
    take_data_hh >> say_hello
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import requests
import json


import requests
import json

def captura_dados():
    url = "https://api.thedogapi.com/v1/images/search?format=json&limit=10"

    payload={}
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': 'live_Sb0XO2cPIA6ekiQHWl2WyCWWP80NHf3z6bQKmufbyWDyntDXuisFOKKXWIS1nY33'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    breeds = json.loads(response.text)

    return [json.dumps(breed_info) for breed_info in breeds if 'breeds' in breed_info]

def filtrar_os_dados(ti):
    breeds = ti.xcom_pull(task_ids='captura_dados')  # Obtém a lista de dicionários
    print('DICTY:', breeds)
    dogs_list = []

    for info in breeds:
        breed_info = json.loads(info)
        breed_list = breed_info.get('breeds', [])
        for breed in breed_list:
            dog_id = breed_info.get('id', 'nothing')
            name = breed.get('name')
            life_span = breed.get('life_span')
            temperament = breed.get('temperament')
            bred_for = breed.get('bred_for')
            url = breed_info.get('url')
 
            dict_dogs = {
                "id": dog_id,
                "nome": name,
                "expectativa_vida": life_span,
                "temperamento": temperament,
                "feito_para": bred_for,
                "url": url
            }
            dogs_list.append(dict_dogs)
        
    print("Resultado:", dogs_list)

    ti.xcom_push(key="dogs_list", value=dogs_list)

def converter_para_excel(ti):
    coleta = ti.xcom_pull(key='dogs_list', task_ids='filtrar_os_dados')
    df = pd.DataFrame.from_dict(coleta)
    print("DF",df)
    df.to_excel("./dags/output/Arquivo_convertido.xlsx")

with DAG('primeira_tentativa', start_date= datetime(2023, 6, 1),
         schedule_interval='30 * * * *', catchup=False) as dag:
    
    captura_dados = PythonOperator(
        task_id = 'captura_dados',
        python_callable= captura_dados,
        provide_context= True,
        dag=dag
    )
    
    filtrar_os_dados = PythonOperator(
        task_id = 'filtrar_os_dados',
        python_callable= filtrar_os_dados,
        provide_context=True,
        dag=dag
    )

    converter_para_excel = PythonOperator(
        task_id = 'converter_para_excel',
        python_callable= converter_para_excel,
        provide_context=True,
        dag=dag
    )

    captura_dados >> filtrar_os_dados >> converter_para_excel








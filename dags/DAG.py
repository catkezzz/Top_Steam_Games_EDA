import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import numpy as np
import psycopg2 as db
from elasticsearch import Elasticsearch


def queryPostgresql():
    '''
    Fungsi ini bertujuan untuk mengambil data dari postgresSQL, ditelusuri menggunakan query,
    lalu diubah menjadi csv
    '''
    connection = db.connect(
            database = 'airflow',
            user = 'airflow',
            password = 'airflow',
            host = 'postgres',
            port = '5432'             
        )
    # Get all data
    select_query = 'SELECT * FROM table_m3'
    df = pd.read_sql(select_query, connection)
    df.to_csv('/opt/airflow/dags/P2M3_catherine_kezia_data_raw.csv', index=False)
    connection.close()

def dataCleaning():
    '''
    Fungsi ini bertujuan untuk membersihkan data, baik itu dari duplicate, mengubah nama kolon
    menjadi format yang sesuai, dan handling missing value
    '''
    df = pd.read_csv('/opt/airflow/dags/P2M3_catherine_kezia_data_raw.csv')

    # Menghapus duplikat
    df_cleaned = df.drop_duplicates()

    # Mengubah nama kolom menjadi lowercase, spasi jadi underscore, menghapus white space, menghilangkan simbol
    df_cleaned.columns = df_cleaned.columns.str.strip().str.lower().str.replace(' ', '_')
    df_cleaned.columns = df_cleaned.columns.str.replace(r'[^a-zA-Z0-9_]', '', regex=True)

    # 'publishers' yang kosong isi dengan 'None'
    df_cleaned['publishers'].fillna('None', inplace=True)

    # jika 'developer' kosong isi dengan nilai 'Anonymous'
    df_cleaned['developers'].fillna('Anonymous', inplace=True)

    # Hapus jika ada kolom lainnya yang kosong
    df_cleaned = df_cleaned.dropna()

    # Mengubah tipe data string di releasedate menjadi DateTime
    df_cleaned['releasedate'] = pd.to_datetime(df['releasedate'], format='%d-%m-%Y')

    df_cleaned.to_csv('/opt/airflow/dags/P2M3_catherine_kezia_data_clean.csv', index=False)


def insertElasticsearch():
    '''
    Fungsi ini bertujuan untuk mengambil data yang telah dibersihkan ke Elastic
    Search sehingga bisa diakses di Kibana
    '''
    es = Elasticsearch('http://elasticsearch:9200')
    df = pd.read_csv('/opt/airflow/dags/P2M3_catherine_kezia_data_clean.csv')
    print('Connection status : ', es.ping())

    failed_insert = []

    for i, r in df.iterrows():
        doc = r.to_json() 
        doc_id = str(r.get('data_m3', i))  
        try:
            print(f"Inserting document {i}, target: {r.get('target', 'No target')}")
            res = es.index(index='data_m3', id=doc_id, doc_type="_doc", body=doc)
        except Exception as e:
            print(f'Index Failed: {r.get("data_m3", "No index")}, Error: {str(e)}')
            failed_insert.append(doc_id) 
            continue 
    print('DONE')
    print('Failed Insert:', failed_insert)


default_args = {
    'owner': 'kezia',
    'start_date': dt.datetime(2024, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG('P2M3_catherine_kezia_DAG',
        default_args=default_args,
        schedule_interval='10-30/10 9 * * 6',  # setiap Sabtu jam 09:10 AM - 09:30 AM
        ) as dag:
    
    #untuk memulai proses
    node_start = BashOperator(task_id='starting',
                                bash_command='echo "Starting the process of reading the CSV file to load the data..."')
    
    getData = PythonOperator(task_id='QueryPostgreSQL',
                                python_callable=queryPostgresql)
    
    cleanData = PythonOperator(task_id='DataCleaning',
                                python_callable=dataCleaning)
    
    insertData = PythonOperator(task_id='InsertDataElasticsearch',
                                python_callable=insertElasticsearch)


    node_start >> getData >> cleanData >> insertData

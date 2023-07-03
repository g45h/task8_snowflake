import os
import pandas as pd
import csv
from datetime import timedelta, datetime
from airflow import DAG
from airflow.hooks.filesystem import FSHook
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import snowflake.connector
from dotenv import load_dotenv


filename = '763K_plus_IOS_Apps_Info.csv'
parsed_file = '444.csv'
hook = FSHook('fs_connection')
filepath = os.path.join(hook.get_path(), filename)
filepath_parsed = os.path.join(hook.get_path(), parsed_file)


def snowflake_connection():
    load_dotenv()
    con = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    return con.cursor()


def two_queries(file):
    df = pd.read_csv(file, sep=',', dtype='unicode', encoding='utf-8')
    columns = df.columns.tolist()
    select_query = ""
    copy_query = ""
    awoo = 1
    for i in columns:
        copy_query = copy_query + ", " + str(i)
        select_query = select_query + f'${awoo} AS {i}, '
        awoo = awoo + 1
    select_query = select_query[:-2]
    copy_query = copy_query[2:]
    return select_query, copy_query


select_q, copy_q = two_queries(filepath_parsed)

def csv_to_raw():
    global select_q, copy_q
    query = 'DELETE FROM RAW_TABLE;'
    query1 = 'REMOVE @~/staged;'
    # reusability moment No.2

    query2 = f'PUT file://{filepath_parsed} @~/staged'
    cur = snowflake_connection()
    cur.execute(query)
    cur.execute(query1)
    cur.execute(query2)

    query = f"COPY INTO RAW_TABLE ({copy_q}) FROM (SELECT {select_q} FROM @~/staged) " \
            f"FILE_FORMAT = (FORMAT_NAME = 'my_csv_format') " \
            f"ON_ERROR = 'CONTINUE';"

    cur = snowflake_connection()
    cur.execute(query)
    return True


def raw_to_stage():
    select_q, copy_q
    query = f"INSERT INTO STAGE_TABLE ({copy_q}) SELECT {select_q} FROM RAW_STREAM;"

    cur = snowflake_connection()
    cur.execute(query)
    return True


def stage_to_master():
    select_query, copy_query = two_queries(filepath_parsed)
    query = f"INSERT INTO MASTER_TABLE ({copy_q}) SELECT {select_q} FROM STAGE_STREAM;"

    cur = snowflake_connection()
    cur.execute(query)
    return True


with DAG(
    'snowflakeETL',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='SnowFlake ETL',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['ETL']
) as dag:
    snierzynka_csv_load = PythonOperator(
        task_id='snierzynka_csv_load',
        python_callable=csv_to_raw,
        dag=dag
    )
    raw_to_stage = PythonOperator(
        task_id='raw_to_stage',
        python_callable=raw_to_stage,
        dag=dag
    )
    stage_to_master = PythonOperator(
        task_id='stage_to_master',
        python_callable=stage_to_master,
        dag=dag
    )
    snierzynka_csv_load >> raw_to_stage >> stage_to_master

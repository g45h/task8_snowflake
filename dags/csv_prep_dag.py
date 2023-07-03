import os
import pandas as pd
import csv
from datetime import timedelta, datetime
from airflow import DAG
from airflow.hooks.filesystem import FSHook
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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


def csv_prep():
    df = pd.read_csv(filepath, sep=',', dtype='unicode')
    df = df.drop('_id', axis=1)
    # delete the index column

    cols_to_str = ['IOS_App_Id', 'Title', 'Developer_Name', 'Developer_IOS_Id', 'IOS_Store_Url',
                   'Seller_Official_Website', 'Age_Rating', 'Total_Average_Rating', 'Total_Number_of_Ratings',
                   'Average_Rating_For_Version', 'Number_of_Ratings_For_Version', 'Original_Release_Date',
                   'Current_Version_Release_Date', 'Price_USD', 'Primary_Genre', 'All_Genres', 'Languages',
                   'Description']
    df[cols_to_str] = df[cols_to_str].astype(str)
    # EVERYTHING to prevent misreading the delimiter char (comma)

    cols_to_modify = ['Original_Release_Date', 'Current_Version_Release_Date']
    for col in cols_to_modify:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        df[col] = df[col].apply(
            lambda x: x.strftime('%Y-%m-%dT%H:%M:%S') if not pd.isnull(x) else '1970-01-01T00:00:00')
    df[cols_to_modify] = df[cols_to_modify].replace("0000-00-00T00:00:00Z", "")
    # datetime modification from 0000-00-00T00:00:00Z to 0000-00-00T00:00:00 & setting null value as an empty string

    df.replace({'"': r'\"'}, regex=True, inplace=True)
    # still trying to prevent comma misreading

    max_description_size = 16_777_210
    df['Description_part1'] = df['Description'].str.slice(0, max_description_size)
    df['Description_part2'] = df['Description'].str.slice(max_description_size, 2 * max_description_size)
    df['Description_part3'] = df['Description'].str.slice(2 * max_description_size, 3 * max_description_size)
    df = df.drop(columns=['Description'])
    # we do have big a$$ string 3x times larger than the max Snowflake STRING capacity. we do address it

    my_columns2 = ['IOS_App_Id', 'Title', 'Developer_Name', 'Developer_IOS_Id', 'IOS_Store_Url',
                   'Seller_Official_Website', 'Age_Rating', 'Total_Average_Rating', 'Total_Number_of_Ratings',
                   'Average_Rating_For_Version', 'Number_of_Ratings_For_Version', 'Original_Release_Date',
                   'Current_Version_Release_Date', 'Price_USD', 'Primary_Genre', 'All_Genres', 'Languages',
                   'Description_part1', 'Description_part2', 'Description_part3']
    df = df[my_columns2]
    # since Snowflake likes to assume we have 21 columns and not 18 I need to explicitly address it

    if os.path.exists(filepath_parsed):
        os.remove(filepath_parsed)
    # reusability moment

    df.to_csv(filepath_parsed, sep=',', index=False, quoting=csv.QUOTE_ALL)
    return True


with DAG(
    'prep',
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
    sensor_task = FileSensor(
        task_id='fs_connection',
        fs_conn_id='fs_connection',
        filepath=filename,
        poke_interval=10,
        dag=dag
    )
    csv_prep = PythonOperator(
        task_id='csv_prep',
        python_callable=csv_prep,
        provide_context=True,
        dag=dag
    )
    trigger = TriggerDagRunOperator(
        task_id='test_trigger_dagrun',
        trigger_dag_id="snowflakeETL",
        dag=dag
    )
    sensor_task >> csv_prep >> trigger

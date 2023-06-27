import os
import pandas as pd
from datetime import timedelta, datetime
from airflow import DAG
from airflow.hooks.filesystem import FSHook
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.sensors.filesystem import FileSensor


filename = '763K_plus_IOS_Apps_Info.csv'
hook = FSHook('fs_connection')
filepath = os.path.join(hook.get_path(), filename)


def remove_indexes(**context):
    df = pd.read_csv(filepath, sep=',', on_bad_lines='skip', index_col=False, dtype='unicode')
    df = df.iloc[:, 1:]
    df.to_csv(filename, index=False)
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
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['ETL'],
) as dag:
    sensor_task = FileSensor(
        task_id='fs_connection',
        fs_conn_id='fs_connection',
        filepath=filename,
        poke_interval=10,
        dag=dag
    )
    indexes_task = PythonOperator(
        task_id='indexes_task',
        python_callable=remove_indexes,
        provide_context=True,
        dag=dag
    )
    sql = "COPY INTO RAW_TABLE FROM '" + filepath + "' FILE_FORMAT=(FORMAT_NAME='csv_format' SKIP_HEADER=1)"
    snierzynka_csv_load = SnowflakeOperator(
        task_id='snierzynka_csv_load',
        snowflake_conn_id='snowflake_conn',
        sql=[sql,
             'INSERT INTO RAW_STREAM SELECT STREAM * FROM RAW_TABLE;'],
        dag=dag
    )
    write_to_stage_table = SnowflakeOperator(
        task_id='write_to_stage_table',
        snowflake_conn_id='snowflake_conn',
        sql=['INSERT INTO STAGE_TABLE SELECT * FROM RAW_STREAM',
             'INSERT INTO STAGE_STREAM SELECT STREAM * FROM STAGE_TABLE;'],
        autocommit=True,
        dag=dag
    )
    write_to_master_table = SnowflakeOperator(
        task_id='write_to_master_table',
        sql='INSERT INTO MASTER_TABLE SELECT * FROM STAGE_STREAM',
        snowflake_conn_id='snowflake_conn',
        database='PRACTICE',
        warehouse='COMPUTE_WH',
        dag=dag
    )

    sensor_task >> indexes_task >> snierzynka_csv_load >> write_to_stage_table >> write_to_master_table

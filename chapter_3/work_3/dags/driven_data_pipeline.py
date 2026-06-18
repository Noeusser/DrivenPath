from airflow import DAG
from airflow.operators.bash import BashOperator #type:ignore
from airflow.operators.python import PythonOperator #type:ignore
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

from batch_generator2 import save_raw_data

# Definir los argumentos predeterminados para DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

# Definir el DAG
dag = DAG(
    'extract_raw_data_pipeline',
    default_args = default_args,
    description = 'DataDriven Main Pipeline',
    schedule_interval = "* 7 * * *",
    start_date=datetime(2024, 9, 22),
    catchup= False,
)

# Definir la tarea de extracción de datos sin procesar
extract_raw_data_task = PythonOperator(
    task_id='extract_raw_data',
    python_callable=save_raw_data,
    dag=dag,
)

# Definir la tarea de creación de esquema sin procesar
create_raw_schema_task=SQLExecuteQueryOperator(
    task_id='create_raw_chema',
    conn_id='postgres_conn',
    sql='CREATE SCHEMA IF NOT EXISTS driven_raw;',
    dag=dag,
)
# Definir la tarea de creación de tabla sin formato
create_raw_table_task = SQLExecuteQueryOperator(
    task_id='create_raw_table',
    conn_id='postgres_conn',
    sql="""
        CREATE TABLE IF NOT EXISTS driven_raw.raw_batch_data (
            person_name VARCHAR(100),
            user_name VARCHAR(100),
            email VARCHAR(100),
            personal_number NUMERIC, 
            birth_date VARCHAR(100), 
            address VARCHAR(100),
            phone VARCHAR(100), 
            mac_address VARCHAR(100),
            ip_address VARCHAR(100),
            clabe VARCHAR(100),
            accessed_at TIMESTAMP,
            session_duration INT,
            download_speed INT,
            upload_speed INT,
            consumed_traffic INT,
            unique_id VARCHAR(100)
        );
    """,
    dag=dag,
)

# Definir la tarea de cargar datos CSV en la tabla

load_raw_data_task = SQLExecuteQueryOperator(
    task_id='load_raw_data',
    conn_id='postgres_conn',
    sql="""
    COPY driven_raw.raw_batch_data(
    person_name, user_name, email, personal_number, birth_date,
    address, phone, mac_address, ip_address, clabe, accessed_at,
    session_duration, download_speed, upload_speed, consumed_traffic, unique_id
    ) 
    FROM '/opt/airflow/data/raw_data.csv' 
    DELIMITER ',' 
    CSV HEADER;
    """,
    dag=dag,
)

# Definir la tarea de ejecución de modelos dbt de puesta en escena

run_dbt_staging_task = BashOperator(
    task_id='run_dbt_staging',
    bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:staging',
)

# Definir la tarea de ejecución de modelos dbt de confianza
run_dbt_trusted_task = BashOperator(
    task_id='run_dbt_trusted',
    bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:trusted',
)

# Establezca la tarea en el DAG.

[extract_raw_data_task, create_raw_schema_task] >> create_raw_table_task
create_raw_table_task >> load_raw_data_task >> run_dbt_staging_task
run_dbt_staging_task >> run_dbt_trusted_task
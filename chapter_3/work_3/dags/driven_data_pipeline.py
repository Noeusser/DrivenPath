import os
import random
import csv
import logging
import uuid
import polars as pl

from faker import Faker
from datetime import date, datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Configure logging.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler()]
)

# Ruta fija del archivo CSV
CSV_PATH = "/opt/airflow/data/raw_data.csv"


def create_data(locale: str) -> Faker:
    """
    Crea una instancia de Faker para generar datos falsos localizados.
    """
    logging.info(f"Created synthetic data for {locale.split('_')[-1]} country code.")
    return Faker(locale)


def generate_record(fake: Faker) -> list:
    """
    Genera un único registro de usuario falso.
    """
    person_name = fake.name()
    user_name = person_name.replace(" ", "").lower()
    email = f"{user_name}@{fake.free_email_domain()}"
    personal_number = fake.ssn()
    birth_date = fake.date_of_birth()
    address = fake.address().replace("\n", ", ")
    phone_number = fake.phone_number()
    mac_address = fake.mac_address()
    ip_address = fake.ipv4()
    clabe = fake.clabe()
    accessed_at = fake.date_time_between("-1y")
    session_duration = random.randint(0, 36_000)
    download_speed = random.randint(0, 1_000)
    upload_speed = random.randint(0, 800)
    consumed_traffic = random.randint(0, 2_000_000)

    return [
        person_name, user_name, email, personal_number, birth_date,
        address, phone_number, mac_address, ip_address, clabe, accessed_at,
        session_duration, download_speed, upload_speed, consumed_traffic
    ]


def write_to_csv() -> None:
    """
    Genera múltiples registros de usuarios falsos y los escribe en un archivo CSV.
    """
    # FIX: Crear el directorio si no existe
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

    fake = create_data("es_MX")

    headers = [
        "person_name", "user_name", "email", "personal_number", "birth_date", "address",
        "phone", "mac_address", "ip_address", "clabe", "accessed_at",
        "session_duration", "download_speed", "upload_speed", "consumed_traffic"
    ]

    rows = random.randint(0, 1_101)

    with open(CSV_PATH, mode="w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        for _ in range(rows):
            writer.writerow(generate_record(fake))

    logging.info(f"Written {rows} records to the CSV file.")


def add_id() -> None:
    """
    Agrega un UUID único a cada fila en el CSV.
    """
    df = pl.read_csv(CSV_PATH)
    uuid_list = [str(uuid.uuid4()) for _ in range(df.height)]
    df = df.with_columns(pl.Series("unique_id", uuid_list))
    df.write_csv(CSV_PATH)
    logging.info("Added UUID to the dataset.")


def update_datetime(run: str) -> None:
    """
    Actualiza la columna 'accessed_at' con la marca de tiempo de ayer.
    """
    if run == "next":
        current_time = datetime.now().replace(microsecond=0)
        yesterday_time = str(current_time - timedelta(days=1))
        df = pl.read_csv(CSV_PATH)
        df = df.with_columns(pl.lit(yesterday_time).alias("accessed_at"))
        df.write_csv(CSV_PATH)
        logging.info("Updated accessed timestamp.")


def save_raw_data() -> None:
    """
    Orquesta la generación, enriquecimiento y guardado del batch diario.
    """
    logging.info(f"Started batch processing for {date.today()}.")
    write_to_csv()
    add_id()
    update_datetime("next")  # FIX: argumento requerido
    logging.info(f"Finish batch processing {date.today()}.")


if __name__ == "__main__":
    save_raw_data()

# ── DAG definition ──────────────────────────────────────────────────────────

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

dag = DAG(
    'extract_raw_data_pipeline',
    default_args=default_args,
    description='DataDriven Main Pipeline',
    schedule_interval="0 7 * * *",  # FIX: "* 7 * * *" ejecutaba cada minuto de la hora 7
    start_date=datetime(2026, 6, 15),
    catchup=False,
)

# Tarea 1: Extraer y guardar datos raw
extract_raw_data_task = PythonOperator(
    task_id='extract_raw_data',
    python_callable=save_raw_data,
    dag=dag,
)

# Tarea 2: Crear schema en Postgres
create_raw_schema_task = SQLExecuteQueryOperator(
    task_id='create_raw_schema',  # FIX: corregido typo "create_raw_chema"
    conn_id='postgres_conn',
    sql='CREATE SCHEMA IF NOT EXISTS driven_raw;',
    dag=dag,
)

# Tarea 3: Crear tabla (DROP + CREATE para garantizar estructura actualizada)
create_raw_table_task = SQLExecuteQueryOperator(
    task_id='create_raw_table',
    conn_id='postgres_conn',
    sql="""
        CREATE TABLE IF NOT EXISTS driven_raw.raw_batch_data (
            person_name     VARCHAR(200),
            user_name       VARCHAR(200),
            email           VARCHAR(200),
            personal_number NUMERIC,
            birth_date      VARCHAR(100),
            address         VARCHAR(500),  -- FIX: ampliado de 100 a 500
            phone           VARCHAR(100),
            mac_address     VARCHAR(100),
            ip_address      VARCHAR(100),
            clabe           VARCHAR(100),
            accessed_at     TIMESTAMP,
            session_duration INT,
            download_speed  INT,
            upload_speed    INT,
            consumed_traffic INT,
            unique_id       VARCHAR(100)
        );
    """,
    dag=dag,
)

# Tarea 4: Cargar CSV en Postgres
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

# Tarea 5: Ejecutar modelos dbt staging
run_dbt_staging_task = BashOperator(
    task_id='run_dbt_staging',
    bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:staging',
    dag=dag,
)

# Tarea 6: Ejecutar modelos dbt trusted
run_dbt_trusted_task = BashOperator(
    task_id='run_dbt_trusted',
    bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:trusted',
    dag=dag,
)

# ── Dependencias ─────────────────────────────────────────────────────────────
[extract_raw_data_task, create_raw_schema_task] >> create_raw_table_task
create_raw_table_task >> load_raw_data_task >> run_dbt_staging_task
run_dbt_staging_task >> run_dbt_trusted_task

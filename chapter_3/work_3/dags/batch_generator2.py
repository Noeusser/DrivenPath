import random
import csv
import logging
import uuid
import polars as pl


from faker import Faker
from datetime import date, datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Configure logging.
logging.basicConfig(
    level=logging.INFO,                    
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler()]
)


def create_data(locale: str) -> Faker:
    """
    Crea una instancia de Faker para generar datos falsos localizados.
    Argumentos:
        locale (str): el código local para el idioma/región de datos falsos deseado.
    Devoluciones:
        Faker: una instancia de la clase Faker configurada con la configuración regional especificada.
    """
    # Log the action.
    logging.info(f"Created synthetic data for {locale.split('_')[-1]} country code.")
    return Faker(locale)


def generate_record(fake: Faker) -> list:
    """
    Genera un único registro de usuario falso.
    Argumentos:
        fake (Faker): una instancia de Faker para generar datos aleatorios.
    Devoluciones:
        lista: una lista que contiene varios detalles de usuarios falsos, como nombre, nombre de usuario, correo electrónico, etc.
    """
    # Generate random personal data.
    person_name = fake.name()
    user_name = person_name.replace(" ", "").lower()  # Create a lowercase username without spaces.
    email = f"{user_name}@{fake.free_email_domain()}"  # Combine the username with a random email domain.
    personal_number = fake.ssn()  # Generate a random social security number.
    birth_date = fake.date_of_birth()  # Generate a random birth date.
    address = fake.address().replace("\n", ", ")  # Replace newlines in the address with commas.
    phone_number = fake.phone_number()  # Generate a random phone number.
    mac_address = fake.mac_address()  # Generate a random MAC address.
    ip_address = fake.ipv4()  # Generate a random IPv4 address.
    clabe = fake.clabe()  # Generate a random CLABE.
    accessed_at = fake.date_time_between("-1y")  # Generate a random date within the last year.
    session_duration = random.randint(0, 36_000)  # Random session duration in seconds (up to 10 hours).
    download_speed = random.randint(0, 1_000)  # Random download speed in Mbps.
    upload_speed = random.randint(0, 800)  # Random upload speed in Mbps.
    consumed_traffic = random.randint(0, 2_000_000)  # Random consumed traffic in kB.

    # Return all the generated data as a list.
    return [
        person_name, user_name, email, personal_number, birth_date,
        address, phone_number, mac_address, ip_address, clabe, accessed_at,
        session_duration, download_speed, upload_speed, consumed_traffic
    ]


def write_to_csv() -> None:
    """
    Genera múltiples registros de usuarios falsos y los escribe en un archivo CSV.
    Argumentos:
        file_path (str): la ruta donde se guardará el archivo CSV.
        filas (int): la cantidad de registros de usuario falsos que se generarán.
    """
    # Create a Faker instance with Romanian data.
    fake = create_data("es_MX")
    
    # Define the CSV headers.
    headers = [
        "person_name", "user_name", "email", "personal_number", "birth_date", "address",
        "phone", "mac_address", "ip_address", "clabe", "accessed_at",
        "session_duration", "download_speed", "upload_speed", "consumed_traffic"
    ]

    if str(date.today()) == "2026-06-09":
        rows = random.randint(100_372, 100_372)
    else: rows = random.randint(0, 1_101)

    # Open the CSV file for writing.
    with open("/opt/airflow/data/raw_data.csv", mode="w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        
        # Generate and write each record to the CSV.
        for _ in range(rows):
            writer.writerow(generate_record(fake))
    # Log the action.
    logging.info(f"Written {rows} records to the CSV file.")


def add_id() -> None:
    """
    Agrega un UUID único a cada fila en un archivo CSV.
    Argumentos:
        nombre_archivo (cadena): la ruta al archivo CSV que se procesará.
    """
    # Load the CSV into a Polars DataFrame.
    df = pl.read_csv("/opt/airflow/data/raw_data.csv")
    # Generate a list of UUIDs (one for each row).
    uuid_list = [str(uuid.uuid4()) for _ in range(df.height)]
    # Add a new column with unique IDs.
    df = df.with_columns(pl.Series("unique_id", uuid_list))
    # Save the updated DataFrame back to a CSV.
    df.write_csv("/opt/airflow/data/raw_data.csv")
    # Log the action.
    logging.info("Added UUID to the dataset.")


def update_datetime(run: str) -> None:
    """
    Actualice la columna 'accessed_at' en un archivo CSV con la marca de tiempo adecuada.
    Argumentos:
        nombre_archivo (cadena): la ruta al archivo CSV que se actualizará.
        ejecutar (cadena): especifica la marca de tiempo que se utilizará.
    """
    if run == "next":
        # Get the current time without milliseconds and calculate yesterday's time.
        current_time = datetime.now().replace(microsecond=0)
        yesterday_time = str(current_time - timedelta(days=1))
        # Load the CSV into a Polars DataFrame.
        df = pl.read_csv("/opt/airflow/data/raw_data.csv")
        # Replace all values in the 'accessed_at' column with yesterday's timestamp.
        df = df.with_columns(pl.lit(yesterday_time).alias("accessed_at"))
        # Save the updated DataFrame back to a CSV file.
        df.write_csv("/opt/airflow/data/raw_data.csv")
        # Log the action.
        logging.info("Updated accessed timestamp.")


def save_raw_data():


    # Logging starting of the process.
    logging.info(f"Started batch processing for {date.today()}.")

    # Define the output file name with today's date.
    output_file = f"chapter_2/evidence/batch_{date.today()}.csv"
    write_to_csv()
    add_id()
    update_datetime()
    logging.info(f"finish batch processing {date.today()}.")

    # Define number of records: first run - 10_372; next runs random number.
    if str(date.today()) == "2026-05-29":
        records = random.randint(100_372, 100_372)
        run_type = "first"
    else:
        records = random.randint(0, 1_101)
        run_type = "next"
    
    # Generate and write records to the CSV.
    write_to_csv(f"{output_file}", records)

    # Add UUID to dataset.
    add_id(output_file)

    # Update the timestamp.
    update_datetime(output_file, run_type)

    # Logging ending of the process.
    logging.info(f"Finish batch processing {date.today()}.")


if __name__ == "__main__":
    save_raw_data()
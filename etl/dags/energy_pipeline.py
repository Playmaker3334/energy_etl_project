from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Agregar la carpeta scripts al path para poder importar los módulos
# Esto es necesario porque Docker monta 'scripts' dentro de dags o al nivel que definimos
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Importar nuestros scripts personalizados
from scripts.generator import generate_energy_data
from scripts.transformer import transform_energy_data

# Argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
with DAG(
    'energy_optimization_etl',
    default_args=default_args,
    description='ETL para análisis de consumo energético y solar',
    schedule_interval='@daily', # Ejecutar una vez al día
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['energy', 'environment', 'etl'],
) as dag:

    # Tarea 1: Extracción (Generar datos sintéticos)
    extract_task = PythonOperator(
        task_id='extract_generate_data',
        python_callable=generate_energy_data,
        doc_md="Genera datos horarios de consumo y energía solar simulando sensores IoT."
    )

    # Tarea 2: Transformación y Carga (Limpiar y guardar Parquet)
    transform_task = PythonOperator(
        task_id='transform_load_data',
        python_callable=transform_energy_data,
        doc_md="Limpia nulos, calcula carga neta y guarda en formato Parquet optimizado."
    )

    # Definir dependencias (Extract -> Transform)
    extract_task >> transform_task
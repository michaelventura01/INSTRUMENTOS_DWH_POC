from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from carga.staging_operation import preparar_extraccion_staging, truncar_tablas_staging, cargar_staging
from carga.dwh_operation import preparar_dwh, cargar_dwh

# Crear el DAG
dag = DAG(
    'extract_load_transform_dag',
    description='DAG para extraer, cargar y transformar datos',
    schedule_interval='@daily',  # Tareas diarias
    start_date=datetime(2023, 1, 1),
    catchup=False
)

# Definir las funciones de cada paso
def paso_1_preparar_extraccion_staging():
    preparar_extraccion_staging()

def paso_2_truncar_tablas_staging():
    truncar_tablas_staging()

def paso_3_cargar_staging():
    cargar_staging()

def paso_4_preparar_dwh():
    preparar_dwh()

def paso_5_cargar_dwh():
    cargar_dwh()

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime(2025, 3, 13),
}

dag = DAG(
    'pipeline_completo',
    default_args=default_args,
    description='Pipeline completo de Staging y DWH',
    schedule_interval='@daily',  
)


extraer_staging = PythonOperator(
    task_id='preparar_extraccion_staging',
    python_callable=paso_1_preparar_extraccion_staging,
    dag=dag,
)

truncar_staging = PythonOperator(
    task_id='truncar_tablas_staging',
    python_callable=paso_2_truncar_tablas_staging,
    dag=dag,
)

cargar_Staging = PythonOperator(
    task_id='cargar_staging',
    python_callable=paso_3_cargar_staging,
    dag=dag,
)

preparar_dwh_stg = PythonOperator(
    task_id='preparar_dwh',
    python_callable=paso_4_preparar_dwh,
    dag=dag,
)

cargar_dwh_stg = PythonOperator(
    task_id='cargar_dwh',
    python_callable=paso_5_cargar_dwh,
    dag=dag,
)

generar_resumenes = BashOperator(
    task_id='generar_resumenes',
    bash_command='psql -h postgres -U admin -d INSTRUMENTOS_DWH -f /scripts/resumen_script.sql',
    dag=dag,
)

extraer_staging >> truncar_staging >> cargar_Staging >> preparar_dwh_stg >> cargar_dwh_stg >> generar_resumenes
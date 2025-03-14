import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import datetime

load_dotenv()

HOST = os.getenv('HOST')
DBNAME = os.getenv('DBNAME')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
PORT=os.getenv('DBPORT')

def get_conexion():
    conexion = psycopg2.connect(
        host=HOST,
        dbname=DBNAME,
        user=USER,
        password=PASSWORD
    )
    return conexion

def get_conexion_string():
    engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}')
    return engine

def cargar_logs(cursor, conexion, schema, tabla):
    log_query = """
        INSERT INTO metadata.process_log (descripcion, etapa, fecha, entidad, estatus)
            VALUES (%s, %s, %s, %s, %s)
    """
    log_values = (f'carga de tabla {tabla}', schema, datetime.datetime.now(), tabla, 'exitoso')
    cursor.execute(log_query, log_values)

    conexion.commit() 

def cargar_log_error(cursor, conexion, schema, tabla, error):
    log_query = """
        INSERT INTO metadata.error_log (descripcion, etapa, fecha, entidad)
            VALUES (%s, %s, %s, %s)
    """
    log_values = (error, schema, datetime.datetime.now(), tabla)
    cursor.execute(log_query, log_values)

    conexion.commit() 
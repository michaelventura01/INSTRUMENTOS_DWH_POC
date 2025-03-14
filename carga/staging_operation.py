from .database_operation import *
from extraccion.extraccion_data import *

staging_dataframes = []
esquema = 'staging'

def preparar_extraccion_staging():
    staging_dataframes.append((preparar_inflation(), esquema,'stg_inflation'))
    staging_dataframes.append((preparar_cpi(), esquema, 'stg_cpi'))
    staging_dataframes.append((preparar_cash_flow(), esquema, 'stg_cash_flow'))
    staging_dataframes.append((preparar_overview(), esquema, 'stg_overview'))
    staging_dataframes.append((preparar_income_statement(), esquema, 'stg_income_statement'))
    staging_dataframes.append((preparar_balance(), esquema, 'stg_balance'))
    staging_dataframes.append((preparar_retail(), esquema, 'stg_retail'))

def get_staging_dataframe():
    return staging_dataframes

def truncar_tablas_staging():
    try:
        conexion = get_conexion()
        cursor = conexion.cursor()
        
        truncate_script = ""
        for df, schema, tabla in staging_dataframes:
            truncate_script += f'truncate table {schema}.{tabla} restart identity cascade;'

        cursor.execute(truncate_script)
        conexion.commit()
    except psycopg2.Error as e:
        add_error_logs(f"Error en la operación de truncado: {e}")
        if conexion:
            conexion.rollback()
    finally:
        if cursor:
            cursor.close()
        if conexion:
            conexion.close()

def cargar_staging():    
    try:
        conexion = get_conexion()
        cursor = conexion.cursor()

        for df, schema, tabla in staging_dataframes:
            for i, row in df.iterrows():
                columns = ', '.join(df.columns)
                values = ', '.join([f"'{str(value)}'" if value is not None else "NULL" for value in row])
                query = f"INSERT INTO {schema}.{tabla} ({columns}) VALUES ({values})"
                cursor.execute(query)
            conexion.commit()
            cargar_logs(cursor, conexion, schema, tabla)                               

        cursor.close()
        conexion.close()
        
    except Exception as e:
        add_error_logs(f"Error al cargar los DataFrames: {e}")        
        if conexion:
            conexion.rollback()
        if cursor:
            cursor.close()
        if conexion:
            conexion.close()

def obtener_entidad_staging(tabla):
    engine = get_conexion_string()
    query = f'select * from {esquema}.{tabla}'
    dataframe = pd.read_sql(query,engine)
    return dataframe

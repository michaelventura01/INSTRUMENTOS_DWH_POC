from .database_operation import *
from extraccion.extraccion_data import *
from .staging_operation import *

dwh_dataframes = []
esquema = 'dwh'

def truncar_tablas_dwh():
    try:
        conexion = get_conexion()
        cursor = conexion.cursor()
        
        truncate_script = ""
        for df, schema, tabla in dwh_dataframes:
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

def preparar_dwh():
    balance_stg_df = obtener_entidad_staging('stg_balance')
    balance_stg_df = balance_stg_df.drop('id')
    dwh_dataframes.append((balance_stg_df, esquema,'balance'))
    
    cpi_stg_df = obtener_entidad_staging('stg_cpi')
    cpi_stg_df = cpi_stg_df.drop('id')
    dwh_dataframes.append((cpi_stg_df, esquema,'cpi'))
    
    cash_flow_stg_df = obtener_entidad_staging('stg_cash_flow')
    cash_flow_stg_df = cash_flow_stg_df.drop('id')
    dwh_dataframes.append((cash_flow_stg_df, esquema,'cash_flow'))
    
    overview_stg_df = obtener_entidad_staging('stg_overview') 
    overview_stg_df = overview_stg_df.drop('id')
    dwh_dataframes.append((overview_stg_df, esquema,'overview'))
    
    income_statement_stg_df = obtener_entidad_staging('stg_income_statement')
    income_statement_stg_df = income_statement_stg_df.drop('id')
    dwh_dataframes.append((income_statement_stg_df, esquema,'income_statement'))
    
    inflation_stg_df = obtener_entidad_staging('stg_inflation')
    inflation_stg_df = inflation_stg_df.drop('id')
    dwh_dataframes.append((inflation_stg_df, esquema,'inflation'))    
    
    retail_stg_df = obtener_entidad_staging('stg_retail')
    retail_stg_df = retail_stg_df.drop('id')
    dwh_dataframes.append((retail_stg_df, esquema, 'retail'))

def get_dwh_dataframe():
    return dwh_dataframes

def cargar_dwh():
    
    try:
        conexion = get_conexion()
        cursor = conexion.cursor()

        for df, schema, tabla in dwh_dataframes:
            for i, row in df.iterrows():
                columns = ', '.join(df.columns)
                values = ', '.join([f"'{str(value)}'" if value is not None else "NULL" for value in row])
                query = f"INSERT INTO {schema}.{tabla} ({columns}) VALUES ({values})"
                cursor.execute(query)
            print(f'{schema}.{tabla}')
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
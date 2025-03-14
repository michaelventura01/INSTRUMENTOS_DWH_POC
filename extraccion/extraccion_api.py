import requests
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

API_KEY = os.getenv('API_KEY')
API_URL = os.getenv('API_URL')
error_logs = []

def get_errors_logs():
    return error_logs

def remove_error_logs():
    error_logs.clear()

def add_error_logs(error):
    error_logs.append(error)

def obtener_simbolos():
    simbolos = {
        'descripcion':[
            'JPMorgan Chase', 'Bank of America', 'Citigroup', 'Wells Fargo', 'Goldman Sachs', 'Morgan Stanley', 'HSBC', 'Barclays'
        ],
        'codigo':[
            'JPM','BAC','C','WFC','GS','MS','HSBC','BCS',
        ]
    }
    simbolos_df = pd.DataFrame(simbolos)
    return simbolos_df

def get_alpha_vantage_data(function, symbol):
  url = f'{API_URL}/query?function={function}&symbol={symbol}&apikey={API_KEY}'
  try:
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    if "Error Message" in data:
      add_error_logs(f"Error para el símbolo {symbol}: {data['Error Message']}")
      return None

    return data

  except requests.exceptions.RequestException as e:
    add_error_logs(f"Error al realizar la solicitud a Alpha Vantage para {symbol}: {e}")
    return None
  except ValueError as e:
    add_error_logs(f"Error al procesar la respuesta JSON {symbol}")
    return None

def get_overview_data_for_multiple_symbols(symbols):
  overview_data = []

  for symbol in symbols:
    data = get_alpha_vantage_data('INCOME_STATEMENT', symbol)
    if data:
      overview_data.append({symbol: data})
    else:
      add_error_logs(f"No se pudieron obtener datos para {symbol}" )
  return overview_data

def get_income_statement_data_for_multiple_symbols(symbols):
  overview_data = []

  for symbol in symbols:
    data = get_alpha_vantage_data('OVERVIEW', symbol)
    if data:
      overview_data.append({symbol: data})
    else:
      add_error_logs(f"No se pudieron obtener datos para {symbol}" )
  return overview_data


def get_balance_sheet_data_for_multiple_symbols(symbols):
  balance_sheet_data = []

  for symbol in symbols:
    data = get_alpha_vantage_data('BALANCE_SHEET', symbol)
    if data:
      balance_sheet_data.append({symbol: data})
    else:
      add_error_logs(f"No se pudieron obtener datos para {symbol}" )
  return balance_sheet_data

def get_cash_flow_data_for_multiple_symbols(symbols):
  cash_flow_data = []

  for symbol in symbols:
    data = get_alpha_vantage_data('CASH_FLOW', symbol)
    if data:
      cash_flow_data.append({symbol: data})
    else:
      add_error_logs(f"No se pudieron obtener datos para {symbol}" )
  return cash_flow_data

def get_cpi_data_for_multiple_symbols(symbols):
  cpi_data = []

  for symbol in symbols:
    data = get_alpha_vantage_data('CPI', symbol)
    if data:
      cpi_data.append({symbol: data})
    else:
      add_error_logs(f"No se pudieron obtener datos para {symbol}" )
  return cpi_data


def get_inflation_data_for_multiple_symbols(symbols):
  inflation_data = []

  for symbol in symbols:
    data = get_alpha_vantage_data('INFLATION', symbol)
    if data:
      inflation_data.append({symbol: data})
    else:
      add_error_logs(f"No se pudieron obtener datos para {symbol}" )
  return inflation_data

def get_retail_sales_data_for_multiple_symbols(symbols):
  retail_sales_data = []

  for symbol in symbols:
    data = get_alpha_vantage_data('RETAIL_SALES', symbol)
    if data:
      retail_sales_data.append({symbol: data})
    else:
      add_error_logs(f"No se pudieron obtener datos para {symbol}" )
  return retail_sales_data
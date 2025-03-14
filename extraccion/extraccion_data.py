from .extraccion_api import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from datetime import date

spark = SparkSession.builder.master("local").appName("Transformaciones").getOrCreate()

simbolos_df = obtener_simbolos()
symbols = simbolos_df['codigo']


def preparar_overview():
    overview_data = get_overview_data_for_multiple_symbols(symbols)
    overview_df = spark.createDataFrame(overview_data)

    overviews_df = spark.createDataFrame([], overview_df.schema)

    for symbol in symbols:
        overview = overview_df.filter(col(symbol).isNotNull()).select(symbol).collect()
        for row in overview:
            overview_item = row[symbol]
            if isinstance(overview_item, dict) or isinstance(overview_item, list):
                if 'annualReports' in overview_item:
                    overviewsdf = spark.createDataFrame(overview_item['annualReports'])
                    overviewsdf = overviewsdf.withColumn("Symbol", lit(symbol)) \
                                             .withColumn("loadDate", lit(date.today()))
                    overviews_df = overviews_df.union(overviewsdf)

    return overviews_df

def preparar_income_statement():
    income_statement_data = get_overview_data_for_multiple_symbols(symbols)
    income_statement_df = spark.createDataFrame(income_statement_data)

    incomestatement_annualReports_df = spark.createDataFrame([], income_statement_df.schema)

    for symbol in symbols:
        incomestatement = income_statement_df.filter(col(symbol).isNotNull()).select(symbol).collect()
        for row in incomestatement:
            incomestatement_item = row[symbol]
            if 'annualReports' in incomestatement_item:
                incomestatement_df = spark.createDataFrame(incomestatement_item['annualReports'])
                incomestatement_df = incomestatement_df.withColumn("Symbol", lit(symbol)) \
                                                       .withColumn("loadDate", lit(date.today()))
                incomestatement_annualReports_df = incomestatement_annualReports_df.union(incomestatement_df)

    return incomestatement_annualReports_df

def preparar_balance():
    balance_data = get_balance_sheet_data_for_multiple_symbols(symbols)
    balance_sheet_df = spark.createDataFrame(balance_data)

    balance_annualReports_df = spark.createDataFrame([], balance_sheet_df.schema)

    for symbol in symbols:
        balance = balance_sheet_df.filter(col(symbol).isNotNull()).select(symbol).collect()
        for row in balance:
            balance_item = row[symbol]
            if 'annualReports' in balance_item:
                balance_df = spark.createDataFrame(balance_item['annualReports'])
                balance_df = balance_df.withColumn("Symbol", lit(symbol)) \
                                       .withColumn("loadDate", lit(date.today()))
                balance_annualReports_df = balance_annualReports_df.union(balance_df)

    return balance_annualReports_df

def preparar_retail():
    retail_sales_data = get_retail_sales_data_for_multiple_symbols(symbols)
    retail_sales_df = spark.createDataFrame(retail_sales_data)

    retaildata_df = spark.createDataFrame([], retail_sales_df.schema)

    for symbol in symbols:
        retail = retail_sales_df.filter(col(symbol).isNotNull()).select(symbol).collect()
        for row in retail:
            retail_item = row[symbol]
            if 'data' in retail_item:
                retail_df = spark.createDataFrame(retail_item['data'])
                retail_df = retail_df.withColumn("name", lit(retail_item['name'])) \
                                     .withColumn("interval", lit(retail_item['interval'])) \
                                     .withColumn("unit", lit(retail_item['unit'])) \
                                     .withColumn("Symbol", lit(symbol)) \
                                     .withColumn("loadDate", lit(date.today()))
                retaildata_df = retaildata_df.union(retail_df)

    return retaildata_df

def preparar_cash_flow():
    cash_flow_data = get_cash_flow_data_for_multiple_symbols(symbols)
    cash_flow_df = spark.createDataFrame(cash_flow_data)

    cashflow_annualreport_df = spark.createDataFrame([], cash_flow_df.schema)

    for symbol in symbols:
        cashflow = cash_flow_df.filter(col(symbol).isNotNull()).select(symbol).collect()
        for row in cashflow:
            cashflow_item = row[symbol]
            if 'annualReports' in cashflow_item:
                cashflow_df = spark.createDataFrame(cashflow_item['annualReports'])
                cashflow_df = cashflow_df.withColumn("Symbol", lit(symbol)) \
                                         .withColumn("loadDate", lit(date.today()))
                cashflow_annualreport_df = cashflow_annualreport_df.union(cashflow_df)

    return cashflow_annualreport_df

def preparar_cpi():
    cpi_data = get_cpi_data_for_multiple_symbols(symbols)
    cpi_df = spark.createDataFrame(cpi_data)

    cpidata_df = spark.createDataFrame([], cpi_df.schema)

    for symbol in symbols:
        cpi = cpi_df.filter(col(symbol).isNotNull()).select(symbol).collect()
        for row in cpi:
            cpi_item = row[symbol]
            if 'data' in cpi_item:
                cpidf = spark.createDataFrame(cpi_item['data'])
                cpidf = cpidf.withColumn("Symbol", lit(symbol)) \
                             .withColumn("name", lit(cpi_item['name'])) \
                             .withColumn("interval", lit(cpi_item['interval'])) \
                             .withColumn("unit", lit(cpi_item['unit'])) \
                             .withColumn("loadDate", lit(date.today()))
                cpidata_df = cpidata_df.union(cpidf)

    return cpidata_df

def preparar_inflation():
    inflation_data = get_inflation_data_for_multiple_symbols(symbols)
    inflation_df = spark.createDataFrame(inflation_data)

    inflationdata_df = spark.createDataFrame([], inflation_df.schema)

    for symbol in symbols:
        inflation = inflation_df.filter(col(symbol).isNotNull()).select(symbol).collect()
        for row in inflation:
            inflation_item = row[symbol]
            if 'data' in inflation_item:
                inflatio_df = spark.createDataFrame(inflation_item['data'])
                inflatio_df = inflatio_df.withColumn("Symbol", lit(symbol)) \
                                         .withColumn("name", lit(inflation_item['name'])) \
                                         .withColumn("interval", lit(inflation_item['interval'])) \
                                         .withColumn("unit", lit(inflation_item['unit'])) \
                                         .withColumn("loadDate", lit(date.today()))
                inflationdata_df = inflationdata_df.union(inflatio_df)

    return inflationdata_df
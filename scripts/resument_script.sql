CREATE TABLE IF NOT EXISTS dwh.resumen_balance_anual AS
SELECT
    symbol,
    EXTRACT(YEAR FROM TO_DATE(fiscaldateending, 'YYYY-MM-DD')) AS year,
    SUM(totalassets) AS total_assets,
    SUM(totalcurrentassets) AS total_current_assets,
    SUM(cashandcashequivalentsatcarryingvalue) AS cash_and_equivalents,
    SUM(totalliabilities) AS total_liabilities,
    SUM(totalshareholderequity) AS total_equity
FROM dwh.balance
GROUP BY symbol, year;

-- Resumen Anual de Cash Flow
CREATE TABLE IF NOT EXISTS dwh.resumen_cash_flow_anual AS
SELECT
    symbol,
    EXTRACT(YEAR FROM TO_DATE(fiscaldateending, 'YYYY-MM-DD')) AS year,
    SUM(operatingcashflow) AS operating_cash_flow,
    SUM(netincome) AS net_income,
    SUM(capitalexpenditures) AS capital_expenditures,
    SUM(dividendpayout) AS dividend_payout
FROM dwh.cash_flow
GROUP BY symbol, year;

-- Resumen Anual de Overview
CREATE TABLE IF NOT EXISTS dwh.resumen_overview_anual AS
SELECT
    symbol,
    EXTRACT(YEAR FROM TO_DATE(fiscaldateending, 'YYYY-MM-DD')) AS year,
    SUM(totalrevenue) AS total_revenue,
    SUM(netincome) AS net_income,
    SUM(ebitda) AS ebitda,
    SUM(grossprofit) AS gross_profit
FROM dwh.overview
GROUP BY symbol, year;

-- Resumen Anual de Income Statement
CREATE TABLE IF NOT EXISTS dwh.resumen_income_statement_anual AS
SELECT
    symbol,
    EXTRACT(YEAR FROM TO_DATE(fiscaldateending, 'YYYY-MM-DD')) AS year,
    SUM(totalrevenue) AS total_revenue,
    SUM(netincome) AS net_income,
    SUM(costofrevenue) AS cost_of_revenue,
    SUM(operatingexpenses) AS operating_expenses
FROM dwh.income_statement
GROUP BY symbol, year;

-- Resumen Anual de CPI
CREATE TABLE IF NOT EXISTS dwh.resumen_cpi_anual AS
SELECT
    symbol,
    EXTRACT(YEAR FROM TO_DATE(date, 'YYYY-MM-DD')) AS year,
    AVG(value) AS avg_cpi_value
FROM dwh.cpi
GROUP BY symbol, year;

-- Resumen Anual de Retail
CREATE TABLE IF NOT EXISTS dwh.resumen_retail_anual AS
SELECT
    symbol,
    EXTRACT(YEAR FROM TO_DATE(date, 'YYYY-MM-DD')) AS year,
    AVG(value) AS avg_retail_value
FROM dwh.retail
GROUP BY symbol, year;

-- Resumen Anual de Inflation
CREATE TABLE IF NOT EXISTS dwh.resumen_inflation_anual AS
SELECT
    symbol,
    EXTRACT(YEAR FROM TO_DATE(date, 'YYYY-MM-DD')) AS year,
    AVG(value) AS avg_inflation_value
FROM dwh.inflation
GROUP BY symbol, year;
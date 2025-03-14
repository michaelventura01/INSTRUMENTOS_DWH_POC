CREATE DATABASE IF NOT EXISTS INSTRUMENTOS_DWH;
\c INSTRUMENTOS_DWH;
create schema if not exists staging;
create schema if not exists dwh;
create schema if not exists metadata;
create schema if not exists diccionario;

create table IF NOT EXISTS staging.stg_overview( 
    id serial primary key
    ,fiscalDateEnding varchar(250)
    ,reportedCurrency varchar(250)
    ,grossProfit varchar(250)
    ,totalRevenue varchar(250)
    ,costOfRevenue varchar(250)
    ,costofGoodsAndServicesSold varchar(250)
    ,operatingIncome varchar(250)
    ,sellingGeneralAndAdministrative varchar(250)
    ,researchAndDevelopment varchar(250)
    ,operatingExpenses varchar(250)
    ,investmentIncomeNet varchar(250)
    ,netInterestIncome varchar(250)
    ,interestIncome varchar(250)
    ,interestExpense varchar(250)
    ,nonInterestIncome varchar(250)
    ,otherNonOperatingIncome varchar(250)
    ,depreciation varchar(250)
    ,depreciationAndAmortization varchar(250)
    ,incomeBeforeTax varchar(250)
    ,incomeTaxExpense varchar(250)
    ,interestAndDebtExpense varchar(250)
    ,netIncomeFromContinuingOperations varchar(250)
    ,comprehensiveIncomeNetOfTax varchar(250)
    ,ebit varchar(250)
    ,ebitda varchar(250)
    ,netIncome varchar(250)
    ,Symbol varchar(250)
    ,loadDate varchar(250)
);

create table IF NOT EXISTS staging.stg_income_statement( 
    id serial primary key
    ,fiscalDateEnding varchar(250)
    ,reportedCurrency varchar(250)
    ,grossProfit varchar(250)
    ,totalRevenue varchar(250)
    ,costOfRevenue varchar(250)
    ,costofGoodsAndServicesSold varchar(250)
    ,operatingIncome varchar(250)
    ,sellingGeneralAndAdministrative varchar(250)
    ,researchAndDevelopment varchar(250)
    ,operatingExpenses varchar(250)
    ,investmentIncomeNet varchar(250)
    ,netInterestIncome varchar(250)
    ,interestIncome varchar(250)
    ,interestExpense varchar(250)
    ,nonInterestIncome varchar(250)
    ,otherNonOperatingIncome varchar(250)
    ,depreciation varchar(250)
    ,depreciationAndAmortization varchar(250)
    ,incomeBeforeTax varchar(250)
    ,incomeTaxExpense varchar(250)
    ,interestAndDebtExpense varchar(250)
    ,netIncomeFromContinuingOperations varchar(250)
    ,comprehensiveIncomeNetOfTax varchar(250)
    ,ebit varchar(250)
    ,ebitda varchar(250)
    ,netIncome varchar(250)
    ,Symbol varchar(250)
    ,loadDate varchar(250)
);

create table IF NOT EXISTS staging.stg_balance( 
    id serial primary key
    ,fiscalDateEnding varchar(250)
    ,reportedCurrency varchar(250)
    ,totalAssets varchar(250)
    ,totalCurrentAssets varchar(250)
    ,cashAndCashEquivalentsAtCarryingValue varchar(250)
    ,cashAndShortTermInvestments varchar(250)
    ,inventory varchar(250)
    ,currentNetReceivables varchar(250)
    ,totalNonCurrentAssets varchar(250)
    ,propertyPlantEquipment varchar(250)
    ,accumulatedDepreciationAmortizationPPE varchar(250)
    ,intangibleAssets varchar(250)
    ,intangibleAssetsExcludingGoodwill varchar(250)
    ,goodwill varchar(250)
    ,investments varchar(250)
    ,longTermInvestments varchar(250)
    ,shortTermInvestments varchar(250)
    ,otherCurrentAssets varchar(250)
    ,otherNonCurrentAssets varchar(250)
    ,totalLiabilities varchar(250)
    ,totalCurrentLiabilities varchar(250)
    ,currentAccountsPayable varchar(250)
    ,deferredRevenue varchar(250)
    ,currentDebt varchar(250)
    ,shortTermDebt varchar(250)
    ,totalNonCurrentLiabilities varchar(250)
    ,capitalLeaseObligations varchar(250)
    ,longTermDebt varchar(250)
    ,currentLongTermDebt varchar(250)
    ,longTermDebtNoncurrent varchar(250)
    ,shortLongTermDebtTotal varchar(250)
    ,otherCurrentLiabilities varchar(250)
    ,otherNonCurrentLiabilities varchar(250)
    ,totalShareholderEquity varchar(250)
    ,treasuryStock varchar(250)
    ,retainedEarnings varchar(250)
    ,commonStock varchar(250)
    ,commonStockSharesOutstanding varchar(250)
    ,Symbol varchar(250)
    ,loadDate varchar(250)
);

create table IF NOT EXISTS staging.stg_retail( 
    id serial primary key
    ,date varchar(250)
    ,value varchar(250)
    ,name varchar(250)
    ,interval varchar(250)
    ,unit varchar(250)
    ,Symbol varchar(250)
    ,loadDate varchar(250)
);


create table IF NOT EXISTS staging.stg_cash_flow( 
    id serial primary key
    ,fiscalDateEnding varchar(250)
    ,reportedCurrency varchar(250)
    ,operatingCashflow varchar(250)
    ,paymentsForOperatingActivities varchar(250)
    ,proceedsFromOperatingActivities varchar(250)
    ,changeInOperatingLiabilities varchar(250)
    ,changeInOperatingAssets varchar(250)
    ,depreciationDepletionAndAmortization varchar(250)
    ,capitalExpenditures varchar(250)
    ,changeInReceivables varchar(250)
    ,changeInInventory varchar(250)
    ,profitLoss varchar(250)
    ,cashflowFromInvestment varchar(250)
    ,cashflowFromFinancing varchar(250)
    ,proceedsFromRepaymentsOfShortTermDebt varchar(250)
    ,paymentsForRepurchaseOfCommonStock varchar(250)
    ,paymentsForRepurchaseOfEquity varchar(250)
    ,paymentsForRepurchaseOfPreferredStock varchar(250)
    ,dividendPayout varchar(250)
    ,dividendPayoutCommonStock varchar(250)
    ,dividendPayoutPreferredStock varchar(250)
    ,proceedsFromIssuanceOfCommonStock varchar(250)
    ,proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet varchar(250)
    ,proceedsFromIssuanceOfPreferredStock varchar(250)
    ,proceedsFromRepurchaseOfEquity varchar(250)
    ,proceedsFromSaleOfTreasuryStock varchar(250)
    ,changeInCashAndCashEquivalents varchar(250)
    ,changeInExchangeRate varchar(250)
    ,netIncome varchar(250)
    ,Symbol varchar(250)
    ,loadDate varchar(250)
);


create table IF NOT EXISTS staging.stg_cpi( 
    id serial primary key
    ,date varchar(250)
    ,value varchar(250)
    ,Symbol varchar(250)
    ,name varchar(250)
    ,interval varchar(250)
    ,unit varchar(250)
    ,loadDate varchar(250)
);

create table IF NOT EXISTS staging.stg_inflation( 
    id serial primary key
    ,date varchar(250)
    ,value varchar(250)
    ,Symbol varchar(250)
    ,name varchar(250)
    ,interval varchar(250)
    ,unit varchar(250)
    ,loadDate varchar(250)
);

create table IF NOT EXISTS diccionario.simbolos(
    id serial PRIMARY key,
    codigo varchar(100) not null,
    descripcion varchar(250) not null,
    fecha_ingreso date not null
)


create table IF NOT EXISTS diccionario.monedas(
    id serial PRIMARY key,
    codigo varchar(100) not null,
    descripcion varchar(250) not null,
    fecha_ingreso date not null
)

create table IF NOT EXISTS metadata.error_log(
    id serial PRIMARY key,
    descripcion varchar(250) not null,
    etapa varchar(250) not null,
    fecha TIMESTAMP not null,
    entidad varchar(250) not null
);

create table IF NOT EXISTS metadata.process_log(
    id serial PRIMARY key,
    descripcion varchar(250) not null,
    etapa varchar(250) not null,
    fecha TIMESTAMP not null,
    entidad varchar(250) not null,
    estatus varchar(250) not null
)

create table IF NOT EXISTS dwh.balance(
    id serial primary key,
    fiscaldateending varchar(250) not null,
    reportedcurrency varchar(250) not null,
    totalassets bigint not null,
    totalcurrentassets bigint not null,
    cashandcashequivalentsatcarryingvalue bigint not null,
    cashandshortterminvestments bigint not null,
    inventory bigint not null,
    currentnetreceivables bigint not null,
    totalnoncurrentassets bigint not null,
    propertyplantequipment bigint not null,
    accumulateddepreciationamortizationppe bigint not null,
    intangibleassets bigint not null,
    intangibleassetsexcludinggoodwill bigint not null,
    goodwill bigint not null,
    investments bigint not null,
    longterminvestments DOUBLE PRECISION not null,
    shortterminvestments DOUBLE PRECISION not null,
    othercurrentassets bigint not null,
    othernoncurrentassets bigint not null,
    totalliabilities bigint not null,
    totalcurrentliabilities DOUBLE PRECISION not null,
    currentaccountspayable bigint not null,
    deferredrevenue bigint not null,
    currentdebt bigint not null,
    shorttermdebt bigint not null,
    totalnoncurrentliabilities bigint not null,
    capitalleaseobligations bigint not null,
    longtermdebt bigint not null,
    currentlongtermdebt DOUBLE PRECISION not null,
    longtermdebtnoncurrent bigint not null,
    shortlongtermdebttotal bigint not null,
    othercurrentliabilities DOUBLE PRECISION not null,
    othernoncurrentliabilities bigint not null,
    totalshareholderequity bigint not null,
    treasurystock bigint not null,
    retainedearnings bigint not null,
    commonstock bigint not null,
    commonstocksharesoutstanding bigint not null,
    symbol varchar(250) not null,
    loaddate varchar(250) not null
)

create table IF NOT EXISTS dwh.cpi(
    id serial primary key,
    date varchar(250) not null,
    value double precision not null,
    symbol varchar(250) not null,
    name varchar(250) not null,
    interval varchar(250) not null,
    unit varchar(250) not null,
    loaddate varchar(250) not null
)

create table IF NOT EXISTS dwh.cash_flow(
    id serial primary key,
    fiscaldateending varchar(250) not null,
    reportedcurrency varchar(250) not null,
    operatingcashflow bigint not null,
    paymentsforoperatingactivities bigint not null,
    proceedsfromoperatingactivities bigint not null,
    changeinoperatingliabilities bigint not null,
    changeinoperatingassets bigint not null,
    depreciationdepletionandamortization bigint not null,
    capitalexpenditures bigint not null,
    changeinreceivables bigint not null,
    changeininventory bigint not null,
    profitloss bigint not null,
    cashflowfrominvestment bigint not null,
    cashflowfromfinancing bigint not null,
    proceedsfromrepaymentsofshorttermdebt bigint not null,
    paymentsforrepurchaseofcommonstock bigint not null,
    paymentsforrepurchaseofequity bigint not null,
    paymentsforrepurchaseofpreferredstock bigint not null,
    dividendpayout bigint not null,
    dividendpayoutcommonstock bigint not null,
    dividendpayoutpreferredstock bigint not null,
    proceedsfromissuanceofcommonstock bigint not null,
    proceedsfromissuanceoflongtermdebtandcapitalsecuritiesnet bigint not null,
    proceedsfromissuanceofpreferredstock bigint not null,
    proceedsfromrepurchaseofequity bigint not null,
    proceedsfromsaleoftreasurystock bigint not null,
    changeincashandcashequivalents bigint not null,
    changeinexchangerate bigint not null,
    totalassets bigint not null,
    netincome bigint not null,
    symbol varchar(250) not null,
    loaddate varchar(250) not null
)

    create table IF NOT EXISTS dwh.overview(
    id serial primary key,
    fiscaldateending varchar(250) not null,
    reportedcurrency varchar(250) not null,
    grossprofit bigint not null,
    totalrevenue bigint not null,
    costofrevenue double precision not null,
    costofgoodsandservicessold double precision not null,
    operatingincome bigint not null,
    sellinggeneralandadministrative bigint not null,
    researchanddevelopment bigint not null,
    operatingexpenses bigint not null,
    investmentincomenet bigint not null,
    netinterestincome bigint not null,
    interestincome bigint not null,
    interestexpense bigint not null,
    noninterestincome bigint not null,
    othernonoperatingincome bigint not null,
    depreciation bigint not null,
    depreciationandamortization bigint not null,
    incomebeforetax bigint not null,
    incometaxexpense bigint not null,
    interestanddebtexpense bigint not null,
    netincomefromcontinuingoperations double precision not null,
    comprehensiveincomenetoftax bigint not null,
    ebit bigint not null,
    ebitda bigint not null,
    netincome bigint not null,
    symbol varchar(250) not null,
    loaddate varchar(250) not null
 )

 create table IF NOT EXISTS dwh.income_statement(
    id serial primary key,
    fiscaldateending varchar(250) not null,
    reportedcurrency varchar(250) not null,
    grossprofit bigint not null,
    totalrevenue bigint not null,
    costofrevenue double precision not null,
    costofgoodsandservicessold double precision not null,
    operatingincome double precision not null,
    sellinggeneralandadministrative bigint not null,
    researchanddevelopment double precision not null,
    operatingexpenses bigint not null,
    investmentincomenet double precision not null,
    netinterestincome double precision not null,
    interestincome double precision not null,
    interestexpense bigint not null,
    noninterestincome double precision not null,
    othernonoperatingincome double precision not null,
    depreciation double precision not null,
    depreciationandamortization bigint not null,
    incomebeforetax bigint not null,
    incometaxexpense bigint not null,
    interestanddebtexpense double precision not null,
    netincomefromcontinuingoperations double precision not null,
    comprehensiveincomenetoftax double precision not null,
    ebit double precision not null,
    ebitda double precision not null,
    netincome bigint not null,
    symbol varchar(250) not null,
    loaddate varchar(250) not null
 )

create table IF NOT EXISTS dwh.retail(
    id serial primary key,
    date varchar(250) not null,
    value double precision not null,
    name varchar(250) not null,
    interval varchar(250) not null,
    unit varchar(250) not null,
    symbol varchar(250) not null,
    loaddate varchar(250) not null
 )

create table IF NOT EXISTS dwh.inflation(
    id serial primary key,
    date varchar(250) not null,
    value double precision not null,
    symbol varchar(250) not null,
    name varchar(250) not null,
    interval varchar(250) not null,
    unit varchar(250) not null,
    loaddate varchar(250) not null
)
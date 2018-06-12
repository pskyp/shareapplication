import json
import pandas as pd
import os
import requests
from sqlalchemy import create_engine
import time

#create the connection to the Data base - might need to change the ODBC connector
engine=create_engine("mssql+pyodbc://pierswilcox:wsShagp1ece@shareserverdb1.database.windows.net:1433/sahredatabase?driver=ODBC+Driver+17+for+SQL+Server")
#gets the list of share codes (Epic), names and index from the database
sharelistdf=pd.read_sql('SELECT * FROM ShareIndex', engine)



#gets all the balnce sheets, names and index from the database
balancesheetdf=pd.read_sql('SELECT * FROM BalanceSheetHistory', engine)
#as already ordered appropritly - take the first instance of each epic and drop the rest
Operatingprofitdf= balancesheetdf.drop_duplicates(subset=['Epic'])


#gets the fundamental history (Epic), names and index from the database
fundamentaldf=pd.read_sql('SELECT * FROM SPFundHistory', engine)
#as already ordered appropritly - take the latest (last) instance of each epic and drop the rest
latestfund = fundamentaldf.drop_duplicates(subset=['Epic'],keep='last')
EV = pd.merge(latestfund, Operatingprofitdf, on='Epic')
EV = EV.apply(pd.to_numeric, errors='ignore')
EV['EntVal']=(EV['MarketCap (m)']+EV['Total of all LIABILITIES']-EV['Cash and Equivalents'])
EV['Cheepness']=((EV['Operating Profit']/EV['EntVal'])*100)
indexedEV=EV.set_index('Epic')
print(indexedEV)
print(indexedEV.dtypes)
indexedEV['Cheepness rank'] = indexedEV['Cheepness'].rank(ascending=False)
indexedEV['Quality rank'] = indexedEV['ROCE (%)'].rank(ascending=False)
indexedEV['Magic score']=((indexedEV['Cheepness rank']+indexedEV['Quality rank']))
indexedEV['Magic rank'] = indexedEV['Magic score'].rank()
indexedEV= indexedEV.sort_values(by='Magic rank')
print(indexedEV[['Name','Operating Profit','EntVal','Cheepness','Cheepness rank','Quality rank','Magic score','Magic rank']])
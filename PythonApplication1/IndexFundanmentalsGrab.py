import json
import pandas as pd
import os
import requests
from sqlalchemy import create_engine

engine=create_engine("mssql+pyodbc://pierswilcox:wsShagp1ece@shareserverdb1.database.windows.net:1433/sahredatabase?driver=ODBC+Driver+17+for+SQL+Server")
ftse100fundurl = "https://shares.telegraph.co.uk/indices/fundamentals/index/UKX"
ftse250fundurl = "https://shares.telegraph.co.uk/indices/fundamentals/index/MCX"
ftse350fundurl = "https://shares.telegraph.co.uk/indices/fundamentals/index/NMX"


ftse100_fundmntlscrape = requests.get(ftse100fundurl)
df= pd.read_html(ftse100_fundmntlscrape.content)[1]
df['date'] = pd.to_datetime('today') 
df=df.drop(df.columns[6], axis=1)
#df.set_index('Epic', inplace = True)
df.to_sql(name='SPFundHistory', con=engine, if_exists='append',index=False, chunksize=50)
print(df)
print(df.dtypes)



ftse250_fundmntlscrape = requests.get(ftse250fundurl)
df= pd.read_html(ftse250_fundmntlscrape.content)[1]
df['date'] = pd.to_datetime('today') 
df=df.drop(df.columns[6], axis=1)
#df.set_index('Epic', inplace = True)
df.to_sql(name='SPFundHistory', con=engine, if_exists='append',index=False, chunksize=50)
print(df)
print(df.dtypes)


ftse350_fundmntlscrape = requests.get(ftse350fundurl)
df= pd.read_html(ftse350_fundmntlscrape.content)[1]
df['date'] = pd.to_datetime('today') 
df=df.drop(df.columns[6], axis=1)
#df.set_index('Epic', inplace = True)
df.to_sql(name='SPFundHistory', con=engine, if_exists='append',index=False, chunksize=50)
print(df)
print(df.dtypes)
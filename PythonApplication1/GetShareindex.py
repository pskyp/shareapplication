import json
import pandas as pd
import os
import requests
from sqlalchemy import create_engine
import time

ftse100url = "https://shares.telegraph.co.uk/indices/?index=UKX"
ftse250url = "https://shares.telegraph.co.uk/indices/?index=MCX"
ftse350url = "https://shares.telegraph.co.uk/indices/?index=NMX"

ftse100 = requests.get(ftse100url, timeout=5)
df100= pd.read_html(ftse100.content)[1]
df100['index'] = 'ftse100'

time.sleep(3)
ftse250 = requests.get(ftse250url, timeout=5)
df250= pd.read_html(ftse250.content)[1]
df250['index'] = 'ftse250'

time.sleep(3)
ftse350 = requests.get(ftse350url, timeout=5)
df350= pd.read_html(ftse350.content)[1]
df350['index'] = 'ftse350'

frames = [df100,df250,df350]
df = pd.concat(frames)
df.reset_index(drop=True)
df = df[['Epic','Name', 'index']]

engine=create_engine("mssql+pyodbc://pierswilcox:wsShagp1ece@shareserverdb1.database.windows.net:1433/sahredatabase?driver=ODBC+Driver+17+for+SQL+Server")
df.to_sql(name='ShareIndex', con=engine, if_exists='replace',index=False, chunksize=50)
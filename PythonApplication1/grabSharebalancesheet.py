import sys
import json
import pandas as pd
import os
import requests
from sqlalchemy import create_engine
import time
import random

#create the connection to the Data base - might need to change the ODBC connector
engine=create_engine("mssql+pyodbc://pierswilcox:wsShagp1ece@shareserverdb1.database.windows.net:1433/sahredatabase?driver=ODBC+Driver+17+for+SQL+Server")
#gets the list of share codes (Epic), names and index from the database
sharelistdf=pd.read_sql('SELECT * FROM ShareIndex', engine)
#takes the Share codes and put them in a list
listofshares=sharelistdf['Epic'].tolist()
#if you want just a portion of the list for testing
first5=listofshares[0:4]
#variable for passing to the loop to scrape all teh URL's for each staock and collate each dataframe produced
dataframelist = []

#loops over list of stock codes and appends the stock code to the dataframe.  Also transposes the dataframe so that years are the rows
def dataframebuilder(value = []):
    for num, Epic in enumerate(listofshares, start=1):
        try:
            shareurl = "https://shares.telegraph.co.uk/fundamentals/?epic={}".format(Epic)   
            sharebalancesheet = requests.get(shareurl, timeout=10)
            df= pd.read_html(sharebalancesheet.content)[9]
            #transposed dataframe
            df=df.T
            #logic to tiddy up transposed frame - removes created headers and resets to next row down for the headers
            header = df.iloc[0]
            df = df[1:]
            df = df.rename(columns = header)
            df.reset_index(level=0, inplace=True)
            # renames one of the column headers
            df.rename(columns={'index': 'Year'}, inplace=True)
            #adds in a column with the Epic name to the dataframe to differeniate
            df['Epic'] = Epic
            #adds the scraped dataframe to the passed list of other scraped dataframes created by the loop
            value.append(df)
            #delay for the scraper so that the web server does not get hammered, random delay to make it look less suspisious
            print('Got '+ str(num) + ' BalanceSheets. Last one was '+ Epic)
            time.sleep(random.randint(3,5))
        except requests.ConnectionError as e:
            print("OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.\n")
            print(str(e))
        except requests.Timeout as e:
            print("OOPS!! Timeout Error")
            print(str(e))
        except requests.RequestException as e:
            print("OOPS!! General Error")
            print(str(e))
        except KeyboardInterrupt:
            print("Someone closed the program")
        except:
            e = sys.exc_info()[0]
            print('something else went wrong with the web scrape')
            print( "<p>Error: %s</p>" % e )
    return value

df = pd.concat(dataframebuilder(dataframelist),sort=False)
Epic = df['Epic']
df.drop(labels=['Epic'], axis=1,inplace = True)
df.insert(0, 'Epic', Epic)
#try:
#    df.to_csv('BalanceSheetHistory.txt', sep='\t')
#except:
#    e = sys.exc_info()[0]
#    print("something went wornt writing the CSV file")
#    print( "<p>Error: %s</p>" % e )
try:
    engine=create_engine("mssql+pyodbc://pierswilcox:wsShagp1ece@shareserverdb1.database.windows.net:1433/sahredatabase?driver=ODBC+Driver+17+for+SQL+Server")
    df.to_sql(name='BalanceSheetHistory', con=engine, if_exists='replace',index=False, chunksize=20)
except:
    e = sys.exc_info()[0]
    print("something went wornt writing to the SQL DB")
    print( "<p>Error: %s</p>" % e )

#shareurl = "https://shares.telegraph.co.uk/fundamentals/?epic=AV."
#sharebalancesheet = requests.get(shareurl, timeout=5)
#df= pd.read_html(sharebalancesheet.content)[9]
##df = df.rename(columns({ ‘Assets £(M)’ : ‘Metric’}))
#df=df.T
#header = df.iloc[0]
#df = df[1:]
#df = df.rename(columns = header)
#df.reset_index(level=0, inplace=True)
#df.rename(columns={'index': 'Year'}, inplace=True)
#print(df)
#print(df.dtypes)
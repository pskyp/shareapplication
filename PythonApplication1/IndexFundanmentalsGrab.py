import sys
import json
import pandas as pd
import os
import requests
from sqlalchemy import create_engine
import time
import random

engine=create_engine("mssql+pyodbc://pierswilcox:wsShagp1ece@shareserverdb1.database.windows.net:1433/sahredatabase?driver=ODBC+Driver+17+for+SQL+Server")
ftse100fundurl = "https://shares.telegraph.co.uk/indices/fundamentals/index/UKX"
ftse250fundurl = "https://shares.telegraph.co.uk/indices/fundamentals/index/MCX"
#ftse350fundurl = "https://shares.telegraph.co.uk/indices/fundamentals/index/NMX"


ftse100_fundmntlscrape = requests.get(ftse100fundurl)
df= pd.read_html(ftse100_fundmntlscrape.content)[1]
df['date'] = pd.to_datetime('today') 
df10=df.drop(df.columns[6], axis=1)
#df.set_index('Epic', inplace = True)
#df.to_sql(name='SPFundHistory', con=engine, if_exists='append',index=False, chunksize=50)
#print(df1)
#print(df.dtypes)



ftse250_fundmntlscrape = requests.get(ftse250fundurl)
df= pd.read_html(ftse250_fundmntlscrape.content)[1]
df['date'] = pd.to_datetime('today') 
df12=df.drop(df.columns[6], axis=1)
#df.set_index('Epic', inplace = True)
#df.to_sql(name='SPFundHistory', con=engine, if_exists='append',index=False, chunksize=50)
#print(df2)
#print(df.dtypes)


#ftse350_fundmntlscrape = requests.get(ftse350fundurl)
#df= pd.read_html(ftse350_fundmntlscrape.content)[1]
#df['date'] = pd.to_datetime('today') 
#df=df.drop(df.columns[6], axis=1)
##df.set_index('Epic', inplace = True)
#df.to_sql(name='SPFundHistory', con=engine, if_exists='append',index=False, chunksize=50)
#print(df)
#print(df.dtypes)

sharelistdf=pd.read_sql('SELECT * FROM ShareIndex', engine)
#puts them alphabetically
sharelistdf= sharelistdf.sort_values(by='Epic')
#takes the Share codes and put them in a list
listofshares=sharelistdf['Epic'].tolist()
#if you want just a portion of the list for testing
first5=listofshares[0:2]
#variable for passing to the loop to scrape all teh URL's for each staock and collate each dataframe produced
dataframelist = []

#loops over list of stock codes and appends the stock code to the dataframe.  Also transposes the dataframe so that years are the rows
def dataframebuilder(value = []):
    for num, Epic in enumerate(listofshares, start=1):
        try:
            shareurl = "https://shares.telegraph.co.uk/fundamentals/?epic={}".format(Epic)   
            sharebalancesheet = requests.get(shareurl, timeout=10)
            
            #Get the sectot and activites disctripion, it's table 3
            
            df5= pd.read_html(sharebalancesheet.content)[2]
            #transposed dataframe
            df5 = df5[[0,1]]

            #logic to tidy up transposed frame - removes created headers and resets to next row down for the headers
            df5=df5.T
            header = df5.iloc[0]
            df5 = df5[1:]
            df5 = df5.rename(columns = header)
            df5.reset_index(level=0, inplace=True)
            df5['Epic'] = Epic
            df5=df5.drop(['Name'], axis=1)
            df5.rename(columns={'Activites': 'Activity'}, inplace=True)

            #Get and re-arrange the key numbers table which is the 4th table
            df= pd.read_html(sharebalancesheet.content)[3]
            #transposed dataframe
            df1 = df[[0,1]]
            df2 = df[[2,3]]
            #logic to tiddy up transposed frame - removes created headers and resets to next row down for the headers
            df1=df1.T
            header = df1.iloc[0]
            df1 = df1[1:]
            df1 = df1.rename(columns = header)
            df1.reset_index(level=0, inplace=True)
            df1['Epic'] = Epic
            df2=df2.T
            header1 = df2.iloc[0]
            #print (df2)
            df2 = df2[1:]
            df2 = df2.rename(columns = header1)
            df2.reset_index(level=0, inplace=True)
            df2['Epic'] = Epic
            frame=[df1,df2]
            # merge the various dataframes together useing the 'Epic' column as a common key
            df6 = pd.merge(df1, df5, on='Epic') 
            df = pd.merge(df6, df2, on='Epic')
            #add share dataframe to the list of other share datadrames created by the loop
            value.append(df)
            ##delay for the scraper so that the web server does not get hammered, random delay to make it look less suspisious
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

#builds a dataframe by concatanating the list of dataframes returned by the function for loop
df3 = pd.concat(dataframebuilder(dataframelist),sort=False)
#merge the fundamental share data grabbed from the 2 index tables and then merge it with the main dataframe from the for loop
result=[df10,df12]
df = pd.concat(result, sort=False)
df=pd.merge(df,df3, on='Epic')
#change the datatypes to numbers where possible
df = df.apply(pd.to_numeric, errors='ignore')
#tidy up by getting rid of some columns that have appeared
df=df.drop(['index_y','index_x'], axis=1)
#save to database, appending to existing table if it exist
df.to_sql(name='SPFundHistory', con=engine, if_exists='append',index=False, chunksize=50)
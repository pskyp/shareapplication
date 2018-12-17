[ippimport json
import pandas as pd
import os
import requests
from sqlalchemy import create_engine
import time
import sys
import random

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
#print(indexedEV)
#print(indexedEV.dtypes)
indexedEV['Cheepness rank'] = indexedEV['Cheepness'].rank(ascending=False)
indexedEV['Quality rank'] = indexedEV['ROCE (%)'].rank(ascending=False)
indexedEV['Magic score']=((indexedEV['Cheepness rank']+indexedEV['Quality rank']))
indexedEV['Magic rank'] = indexedEV['Magic score'].rank()
indexedEV= indexedEV.sort_values(by='Magic rank')


#Gets the EPics of  top 30 shares as ranked by Magic formula so that furher analysis can be run
Targetdf=pd.DataFrame (indexedEV.index.values,columns=['Epic'])
Targetdf=Targetdf.head(30)
#print(Targetdf)

#takes the Share codes and put them in a list
listofshares=Targetdf['Epic'].tolist()
#if you want just a portion of the list for testing
first5=listofshares[0:5]
#variable for passing to the loop to scrape all teh URL's for each staock and collate each dataframe produced
dataframelist = []

#loops over list of stock codes and appends the stock code to the dataframe.  Also transposes the dataframe so that years are the rows
def dataframebuilder(value = []):
    for num, Epic in enumerate(listofshares, start=1):
        try:

            shareurl = "https://quotes.wsj.com/UK/XLON/{}/research-ratings".format(Epic)   
            sharebalancesheet = requests.get(shareurl, timeout=10)
            
            #Get the sector and activites disctripion, it's table 9
            

            df2= pd.read_html(sharebalancesheet.content)[8]
            df2= df2.drop(['3 Months Ago','1 Month Ago'], axis=1)
            #df2=df2['Current']

            df2=df2.T
            header = df2.iloc[0]
            df2 = df2[1:]
            df2 = df2.rename(columns = header)
            df2.reset_index(level=0, inplace=True)
            df2['Epic'] = Epic
            df2= df2.drop(['index'], axis=1)
            value.append(df2)
            ##delay for the scraper so that the web server does not get hammered, random delay to make it look less suspisious
            time.sleep(random.randint(3,5))
            print('Got '+ str(num) + ' BalanceSheets. Last one was '+ Epic)

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

df3 = pd.concat(dataframebuilder(dataframelist),sort=False)
indexedEV = pd.merge(indexedEV,df3,on='Epic')
indexedEV['Analyst Rating']=(((indexedEV['Buy']*3)+(indexedEV['Overweight']*2)+(indexedEV['Hold'])+(indexedEV['Underweight']*-1)+(indexedEV['Sell']*-4)))
indexedEV['Analyst rank'] = indexedEV['Analyst Rating'].rank(ascending=False)
indexedEV['MyScore']=((indexedEV['Analyst rank']+indexedEV['Magic rank']))
indexedEV['Myscore rank'] = indexedEV['MyScore'].rank()
indexedEV= indexedEV.sort_values(by='Myscore rank')
indexedEV = indexedEV.dropna(axis=0,how='all')
indexedEV= indexedEV.drop(['index','nan','P/Eratio',], axis=1)

print(indexedEV)
print(indexedEV.dtypes)

indexedEV.to_csv('MagicScreenHistory.txt', sep='\t')
indexedEV= indexedEV[['Epic','Name','date','Sector','MarketCap (m)','ROCE (%)','Operating Profit','EntVal','Cheepness rank','Quality rank','Magic rank','Consensus','Myscore rank']]
indexedEV.to_sql(name='MagicScreenHistory', con=engine, if_exists='append',index=False, chunksize=50)

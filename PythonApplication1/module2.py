from odo import odo
#dshape = discover(resource('BalanceSheerHistorytidy.txt'))
#engine=create_engine("mssql+pyodbc://pierswilcox:wsShagp1ece@shareserverdb1.database.windows.net:1433/sahredatabase?driver=ODBC+Driver+17+for+SQL+Server")
odo('BalanceSheerHistorytidy.txt', "mssql+pyodbc://pierswilcox:wsShagp1ece@shareserverdb1.database.windows.net:1433/sahredatabase::test?driver=ODBC+Driver+17+for+SQL+Server")

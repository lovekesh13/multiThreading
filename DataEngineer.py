### Disclosure
### I googled the API and found the documentation 
### and used that to code the project

import sqlite3
import os
import requests

import pandas as pd
from sodapy import Socrata

import concurrent.futures
import multiprocessing.dummy as mp
import threading



## Define paths
dbPath = r'C:\Users\Lovekesh'
dbName = 'egenSolTest.db'


## Setup connection & cursor with the DB
dbConn = sqlite3.connect(os.path.join(dbPath, dbName), check_same_thread=False)
# dbCurs = dbConn.cursor()


## Setup the API and bring in the data
client = Socrata("health.data.ny.gov", None)

## Define all the countys to be used in threading
countys = [
    'Albany',
    'Allegany',
    'Bronx',
    'Broome',
    'Cattaraugus',
    'Cayuga',
    'Chautauqua',
    'Chemung',
    'Chenango',
    'Clinton',
    'Columbia',
    'Cortland',
    'Delaware',
    'Dutchess',
    'Erie',
    'Essex','Franklin','Fulton','Genesee','Greene','Hamilton','Herkimer','Jefferson','Kings','Lewis','Livingston','Madison','Monroe','Montgomery','Nassau','New York','Niagara','Oneida','Onondaga','Ontario','Orange','Orleans','Oswego','Otsego','Putnam','Queens','Rensselaer','Richmond','Rockland','St. Lawrence','Saratoga','Schenectady','Schoharie','Schuyler','Seneca','Steuben','Suffolk','Sullivan','Tioga','Tompkins','Ulster','Warren','Washington','Wayne','Westchester','Wyoming','Yates']

varDict = dict.fromkeys(countys, {})
strDataList = ['test_date', 'LoadDate']
intDataList = ['new_positives', 'cumulative_number_of_positives', 'total_number_of_tests', 'cumulative_number_of_tests']


def getData(county):
    
    ## Check if table exists
    print("Processing ", county)
    varDict[county]['dbCurs'] = dbConn.cursor()
    varDict[county]['select'] = varDict[county]['dbCurs'].execute('SELECT name FROM sqlite_master WHERE type="table" AND name=?', (county,) )
    if not len(varDict[county]['select'].fetchall()):
        createTable(county)
    
    ## Get Max date to load data in increments
    # sqlQuery = 'SELECT MAX([Test Date]) FROM '+county
    # select = dbCurs.execute(sqlQuery)
    
    ## Define the where clause
    whereClause = 'county="'+county+'"'
    
    # whereClause = 'test_date="'+x+'"'
    # whereClause = 'test_date="2021-01-29"'
    
    ## Fetch filtered data
    ## Correct the data types
    ## convert to list
    varDict[county]['results'] = client.get("xdss-u53e", where=whereClause)
    varDict[county]['data'] = pd.DataFrame.from_records(varDict[county]['results'])
    varDict[county]['data'].drop(['county'], axis=1, inplace=True)
    varDict[county]['data']['LoadDate'] = pd.to_datetime('now')
    varDict[county]['data'][strDataList] = varDict[county]['data'][strDataList].astype(str)
    varDict[county]['data']['test_date'] = varDict[county]['data']['test_date'].apply(lambda x: x[:10])
    varDict[county]['data'][intDataList] = varDict[county]['data'][intDataList].astype(int)
    varDict[county]['data'] = varDict[county]['data'].values.tolist()
    # dataTu = [tuple(x) for x in data]
    
    ## Insert values into SQLite
    varDict[county]['sqlQuery'] = 'INSERT INTO ['+county+'] VALUES (?,?,?,?,?,?)'
    varDict[county]['dbCurs'].executemany(varDict[county]['sqlQuery'], varDict[county]['data'])
    dbConn.commit()
    
# for i in dbCurs.execute('SELECT * FROM albany'):
#     print(i)
    
    

def createTable(county):
    
    sqlQuery = 'CREATE TABLE ['+county+'] ( [Test Date] TEXT, [New Positives] INTEGER NOT NULL, [Cumulative Number of Positives] INTEGER NOT NULL, [Total Number of Tests Performed] INTEGER NOT NULL, [Cumulative Number of Tests Performed] INTEGER NOT NULL, [Load date] TEXT NOT NULL, PRIMARY KEY([Test Date]))'
    varDict[county]['dbCurs'].execute(sqlQuery)
    

# for _ in countys:
#     getData(_)
    
# x = countys[:5]

with concurrent.futures.ThreadPoolExecutor() as executor:
    # results = [executor.submit(getData, y) for y in x]
    executor.map(getData, countys)
    



## Author: Justin Flick 
## Blue Bottle Coding Challenge

from sqlalchemy import create_engine #ORM library
from dask import dataframe as dd #pararellism library 
import numpy as np #required with Pandas
import pandas as pd #Data manipulation library
import datetime #standard library date handler
import json #standard library JSON parser
import requests #HTTP library

#define global variables
API_KEY = 'eb8328e17df6aa5e22a91c73d132e1d2'# repace with the key provided in challenge documentation

class Database_Config:
    """ Defines the database connection and methods for manipulating the SQLite databse"""
     
    def __init__(self):
        self.db_engine =  create_engine(r'sqlite:///blue_bottle.db', echo=False) #create db
        
    def create_db(self, dataframe, table_name): 
        """Creates a new table in the SQLlite databse from a Dataframe object. It will overwrite the existing table each time."""
        df = dataframe
        df.to_sql(table_name, con=self.db_engine, if_exists='replace')

class ETL(Database_Config):
    """ Class that holds our ETL methods"""
    def __init__(self, path_to_csv):
        Database_Config.__init__(self) #inherits the create_db method and db connection from the Database_Config class
        self.api_key = API_KEY #set from global 
        self.csv = path_to_csv 
        self.csv_dataframe = [] #intilaize empty dataframe
        self.temperature_array = []

    def extract_csv_data(self): 
        """Extract data from the provided sales csv and read into dataframe"""
        csv = pd.read_csv(self.csv, parse_dates=['local_created_at'])
        self.csv_dataframe = csv
      
    def clean_csv(self): 
        """Transform sales dataframe for processing"""
        self.csv_dataframe.dropna #remove empty rows
        self.csv_dataframe = dd.from_pandas(self.csv_dataframe,npartitions=12) #convert to Dask dataframe for Pararell processing

    def call_weather_api(self,date):
        """Invokes the Dark Sky weather api to pull hourly temperature. See documentation here: https://darksky.net/dev/docs"""
        tdate = str(date).replace(' ','T') #format date per DarK Sky API documentation
        coordinates = '37.831106,-122.254110' #morse cafe coordinates
        base_url = 'https://api.darksky.net/forecast/'
        full_url = base_url + self.api_key + "/" + coordinates + "," + tdate + "?exclude=hourly" #build request URL with parameters
        r = requests.get(full_url) #send the request
        result = json.loads(r.text) #load response into JSON parser
        float_temp = (result['currently'])['temperature'] #convert temp format
        temp = int(round(float_temp))
        print("in progress...") #progress indicator
        return temp

    def optimized_weather_data_etl(self):
        """Processes the call_weather_api method using pararellism via a multiprocessing approach"""
        df = self.csv_dataframe
        df['Temperature'] = df.apply(lambda row: self.call_weather_api(row['local_created_at']),axis=1) #adds the temperature to the csv_dataframe. This runs our data partions in pararell.

    def load_data(self):
        df = self.csv_dataframe.compute() #converts partioned Dask dataframe back to a singlular Pandas dataframe
        create_db(df, 'Sales') #create databse we'll use for reporting
        #df.to_csv('test.csv') #testing
        
class Reporting(Database_Config):
    """Class that contains our reporting methods"""

    def __init__(self): 
        Database_Config.__init__(self)  #inherits the create_db method and db connection from the Database_Config class

    def report_1(self): 
        "method to run the Best Seller report"
        query = """SELECT temp as 'Temperature (Whole Degress Farenheit)', item_name as 'Item Name', MAX(Number_Sold) as 'Number Sold' FROM (SELECT temp, item_name as Item_Name,  SUM(net_quantity) as Number_Sold FROM Sales
            GROUP BY temp, item_name
            ORDER BY temp, Number_Sold DESC) 
            GROUP BY temp
            ORDER BY temp ASC
            """
        df = pd.read_sql(query, self.db_engine) #run the query and store results as a dataframe
        df.to_csv("Report_1.csv", index=False) #generate CSV report

    def report_2(self): 
        """method to run the Sales Change by Temperature report. I broke this into two queries to make creating the calculated fields easier"""
        #query to pull item sales by day, rather than hourly 
        query = """SELECT strftime('%Y-%m-%d', local_created_at), item_name, net_quantity, temp FROM Sales GROUP BY item_name, strftime('%Y-%m-%d', local_created_at) ORDER BY  strftime('%Y-%m-%d', local_created_at), item_name DESC"""
        df = pd.read_sql(query, self.db_engine) #run the query and store results ina dataframe
        df["Changes_in_temp"] = df["temp"].diff(-1) #calculate change in temperature between days 
        df["Change_in_sales"] = df['net_quantity'].diff(-1) #calculate change in sales by dayes 
        self.create_db(df, 'Sales_Trends') #stores the modified data frame as a new table for additional manipulation 
        query2 = """Select c.item_name as 'Item Name', c.avg_change_cold as 'Average Change in Sales when Colder' , w.avg_change_warm as 'Average Change in Sales when Warmer'  
        FROM (SELECT item_name, AVG(Change_in_sales) as avg_change_cold FROM Sales_Trends WHERE Changes_in_temp = -2 GROUP BY item_name) as c
        INNER JOIN (SELECT item_name, AVG(Change_in_sales) as avg_change_warm FROM Sales_Trends WHERE Changes_in_temp = 2 GROUP BY item_name) as w ON c.item_name = w.item_name"""
        df2 = pd.read_sql(query2, self.db_engine) #run the query and store results ina dataframe
        df2.to_csv("Report_2.csv", index=False) #generate CSV report
       

def run_reports_only():
    "independent method to only run the reports on the pre-processed database"
    report = Reporting() #intialize reporting method
    report.report_1() # run report 1
    report.report_2() # run report 2 

def run_entire_pipeline():
    """Independent method to run the entire ETL pipeline from start to finish"""
    path_to_csv = "morse.csv" #intial csv
    etl = ETL(path_to_csv) #initialize ETL method
    etl.extract_csv_data() #read csv
    etl.clean_csv() #prepare csv for procesing
    etl.optimized_weather_data_etl() #get temperature data
    etl.load_data() #store in database
    report = Reporting() #intialize reporting method
    report.report_1() #run report 1 
    report.report_2() #run report 2 


if __name__ == '__main__':
    run_reports_only()
    #run_entire_pipeline() #uncomment to run entire pipeline
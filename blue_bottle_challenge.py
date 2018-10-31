import pandas as pd
import datatime 
import requests 




#define global variables

DB_USERNAME = 
DB_PASSWORD = 
API_KEY =


class Database_Config:
    
    def __init__(self):
        self.db_username = DB_USERNAME
        self.db_password = DB_PASSWORD
        
    def create_db(self, array_of_columns): 
        #index primary key

    def intialize_cursor(self):

class ETL(Database_Config):

    def __init__(path_to_csv):

        Database_Config.__init__(self) #inherits the create_db and intialize_cursor methods from the Database_Config class
        self.api_key = API_KEY
        self.csv = path_to_csv
        self.csv_dataframe = {[]}
        self.weather_dataframe = {[]}

    def extract_csv_data(self): 
        #extract csv

    def clean_csv(self): 

        #remove negative values, etc...

    def load_csv_data(self): 
        
        array_of_columns = [] do this 
        self.create_db(array_of_columns)
        cursor = self.intialize_cursor
        cursor.execute "INSERT INTO..."

    def extract_API_data(self)
        #get data for 2016
        
    def clean_API_data(self):
        #convert datetime format to match csv

    def load_api_data(self)

        array_of_columns = [] do this 
        self.create_db(array_of_columns)
        cursor = self.intialize_cursor()
        cursor.execute "INSERT INTO..."

class Reporting(Database_Config):

    def __init__(self): 
        Database_Config.__init__(self)

    def write_report_to_csv(self):
        #export report to csv file

    def report_1(self): 
        #run Report 1
        cursor = self.intialize_cursor()
        cursor.execute("SELECT ....")
        
    def report_2(self): 
        #run Report 2
        cursor = self.intialize_cursor()
        cursor.execute("SELECT ....")


    
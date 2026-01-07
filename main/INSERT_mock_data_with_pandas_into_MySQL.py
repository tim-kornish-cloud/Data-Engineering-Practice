"""
Author: Timothy Kornish
CreatedDate: August - 17 - 2025
Description: Load a csv of mock data into a pandas dataframe.
             log into a MySQL Database.
             insert a subset of rows from the csv into a table in the MySQL DB

"""

import numpy as np
import pandas as pd
import os
from custom_db_utilities import  MySQL_Utilities
from credentials import Credentials

# create and instance of the custom salesforce utilities class used to interact with Salesforce
MySQL_Utils = MySQL_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

# declare which environment this script will perform operations against,
# can have multiple environments in the same script at the same time
env = 'localhost'

# set database to MySQL
database = "MySQL"

# number of records to attempted
num_of_records = 10

# starting index to choose records
record_start = 10

# set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# set up fallout ans success path to save files to
# success file path
success_file = dir_path + "\\Output\\INSERT\\SUCCESS_Insert_" + env + "_" + database + ".csv"
# fallout file path
fallout_file = dir_path + "\\Output\\INSERT\\FALLOUT_Insert_" + env + "_" + database + ".csv"

# set input path for mock data csv
input_csv_file = dir_path + ".\\MockData\\MOCK_DATA_Multi_Data_Types.csv"

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
mock_data_df = pd.read_csv(input_csv_file)

# Convert the datetime column to 'DD-MM-YYYY' string format
mock_data_df['CreatedDate'] = pd.to_datetime(mock_data_df['CreatedDate'], errors = 'ignore', dayfirst = True, format = '%m/%d/%Y')
mock_data_df['CreatedDate'] = mock_data_df['CreatedDate'].dt.date
mock_data_df['CreatedDate'] = mock_data_df['CreatedDate'].astype(str)
# mock_data_df  = mock_data_df[["AccountNumber"
#       , "Name"
#       , "NumberOfEmployees"
#       , "NumberOfLocations__c"
#       , "Phone"
#       , "SLA__c"
#       , "SLASerialNumber__c"
#       , "Account_Number_External_ID__c"
#       , "IsActive"]]
print(mock_data_df.head())

# select only 10 records
df_to_upload = mock_data_df.iloc[record_start:record_start+num_of_records]

server = Cred.get_server(dbms = database, env = env)
database = Cred.get_database(dbms = database, env = env)
username = Cred.get_mysql_username(dbms = database, env = env)
password = Cred.get_mysql_password(dbms = database, env = env)


# initiate a MySQL engine to query with
connection, cursor = MySQL_Utils.login_to_mysql(server = server, database = database,
                                         username = username, password = password)

# select accounts to match against the csv to not attempt to insert duplicates
select_query = """SELECT AccountNumber
      , Name
      , NumberOfEmployees
      , NumberOfLocations__c
      , Phone
      , SLA__c
      , SLASerialNumber__c
      , Account_Number_External_ID__c
      , IsActive
      , CreatedDate
     -- , AmountPaid
  FROM data_engineering.accounts_test_1"""


# accounts in the mssql table shown in the query above
MySQL_Utils.insert_dataframe_into_mysql_table(connection, cursor, df_to_upload, 'accounts_test_1')

# set query to query all the records that are on the table
accounts_query = "SELECT * FROM data_engineering.accounts_test_1"

# query the records inserted
accounts_test_df = MySQL_Utils.query_mysql_return_dataframe(accounts_query, connection)

# print the inserted records
print(accounts_test_df.head())

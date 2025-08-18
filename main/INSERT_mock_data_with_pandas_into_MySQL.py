"""
Author: Timothy Kornish
CreatedDate: August - 17 - 2025
Description: Load a csv of mock data into a pandas dataframe.
             log into a MySQL table.

"""

import numpy as np
import pandas as pd
import os
from custom_db_utilities import  Custom_MySQL_Utilities
from credentials import Credentials

#create and instance of the custom salesforce utilities class used to interact with Salesforce
MySQL_Utils = Custom_MySQL_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

# declare which environment this script will perform operations against,
# can have multiple environments in the same script at the same time
environment = 'localhost'

#number of records to attempted
num_of_records = 100

#starting index to choose records
record_start = 80

#set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# set up fallout ans success path to save files to
# success file path
success_file = dir_path + "\\Output\\INSERT\\SUCCESS_Insert_" + environment + ".csv"
# fallout file path
fallout_file = dir_path + "\\Output\\INSERT\\FALLOUT_Insert_" + environment + ".csv"

# set input path for mock data csv
input_csv_file = dir_path + ".\\MockData\\MOCK_DATA.csv"

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
mock_data_df = pd.read_csv(input_csv_file)

# select only 10 records
df_to_upload = mock_data_df.iloc[record_start:record_start+num_of_records]

#set environment to localhost and database to MySQL
env = "localhost"
database = "MySQL"

#initiate a MySQL engine to query with
connection = MySQL_Utils.login_to_MySQL(server = Cred.get_server(dbms = database, env = env), database = Cred.get_database(dbms = database, env = env),
                                username = Cred.get_username(dbms = database, env = env), password = Cred.get_password(dbms = database, env = env))


# select accounts to match against the csv to not attempt to insert duplicates
select_query = """SELECT AccountNumber
      , Name
      , NumberOfEmployees
      , NumberOfLocations__c
      , Phone
      , SLA__c
      , SLASerialNumber__c
      , Account_Number_External_ID__c
  FROM data_engineering.accounts_test_1"""


#accounts in the mssql table shown in the query above
MySQL_Utils.insert_dataframe_into_MySQL_table(connection, df_to_upload, 'accounts_test_1', if_exists = 'append')

# set query to query all the records that are on the table
accounts_query = "SELECT * FROM data_engineering.accounts_test_1"

# query the records inserted
accounts_test_df = MySQL_Utils.query_MySQL_return_DataFrame(accounts_query, connection)
# print the inserted records
print(accounts_test_df.head())

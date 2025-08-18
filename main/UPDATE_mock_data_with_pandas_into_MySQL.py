"""
Author: Timothy Kornish
CreatedDate: August - 17 - 2025
Description: Load a csv of mock data into a pandas dataframe.
             log into a MySQL table.

"""

import numpy as np
import pandas as pd
import os
from custom_db_utilities import  Custom_MySQL_Utilities, Custom_Utilities
from credentials import Credentials

#create and instance of the custom salesforce utilities class used to interact with Salesforce
MySQL_Utils = Custom_MySQL_Utilities()
#Create instance of Utils class
Utils = Custom_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

# declare which environment this script will perform operations against,
# can have multiple environments in the same script at the same time
environment = 'localhost'

#number of records to attempted
num_of_records = 10

#starting index to choose records
record_start = 90

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

# set query to query all the records that are on the table
accounts_query = "SELECT * FROM data_engineering.accounts_test_1"

# query the records inserted
account_df = MySQL_Utils.query_MySQL_return_DataFrame(accounts_query, connection)

# format the merge column to remove any whitespace
account_df.columns = account_df.columns.str.strip()
df_to_upload.columns = df_to_upload.columns.str.strip()

print(len(account_df))
print(account_df.head())
print(account_df.columns)

print(len(df_to_upload))
print(df_to_upload.head())
print(df_to_upload.columns)

# format the merge column data types to string before joining
account_df['Account_Number_External_ID__c'] = account_df['Account_Number_External_ID__c'].astype(str)
df_to_upload['Account_Number_External_ID__c'] = df_to_upload['Account_Number_External_ID__c'].astype(str)

# get dataframes for records that exist in both df, left and right only
# perform outer join on 'Account_Number_External_ID__c'
both_df, left_only_df, right_only_df = Utils.get_df_diffs(account_df, df_to_upload, left_on = 'Account_Number_External_ID__c', right_on = 'Account_Number_External_ID__c', how = 'outer', suffixes = ('_left', '_right'), indicator = True, validate = None)


# modify account SLA value to gold
# modify number of locations to 5
account_columns_to_update = ['SLA__c', 'NumberOfLocations__c']
account_values_to_update = ["Platinum", "35"]

#set table to update
table_to_update = 'data_engineering.accounts_test_1'

#table key field
table_UID = 'Account_Number_External_ID__c'

# below is a sql list as a single string
accounts_to_update_list = Utils.generate_sql_list_from_df_column(both_df, 'Account_Number_External_ID__c', output = 'string')
#
MySQL_Utils.update_rows_in_MySQL_table(connection, table_to_update,
                                      account_columns_to_update, account_values_to_update,
                                      table_UID, accounts_to_update_list)

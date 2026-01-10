"""
Author: Timothy Kornish
CreatedDate: August - 17 - 2025
Description: Load a csv of mock data into a pandas dataframe.
             log into a MySQL table.
             query the existing records on the Mysql table
             merge the table with the CSV and get a tuple shoping which records match
             perform an update to records in the Mysql table based on the matching IDs.

"""

import numpy as np
import pandas as pd
import os
from custom_db_utilities import  MySQL_Utilities, Custom_Utilities
from credentials import Credentials
import pymysql

# create and instance of the custom salesforce utilities class used to interact with Salesforce
MySQL_Utils = MySQL_Utilities()
# create instance of Utils class
Utils = Custom_Utilities()
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

# select only 10 records
df_to_upload = mock_data_df.iloc[record_start:record_start+num_of_records]

server = Cred.get_server(dbms = database, env = env)
database = Cred.get_database(dbms = database, env = env)
username = Cred.get_mysql_username(dbms = database, env = env)
password = Cred.get_mysql_password(dbms = database, env = env)


# initiate a MySQL engine to query with
connection, cursor = MySQL_Utils.login_to_mysql(server = server, database = database,
                                         username = username, password = password)
# connection = pymysql.connect(host=server,user=username,password=password,database=database)
# cursor = connection.cursor()
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
account_df = MySQL_Utils.query_mysql_return_dataframe(accounts_query, connection)

# format the merge column to remove any whitespace
account_df.columns = account_df.columns.str.strip()
df_to_upload.columns = df_to_upload.columns.str.strip()

# format the merge column data types to string before joining
account_df['Account_Number_External_ID__c'] = account_df['Account_Number_External_ID__c'].astype(str)
df_to_upload['Account_Number_External_ID__c'] = df_to_upload['Account_Number_External_ID__c'].astype(str)

# get dataframes for records that exist in both df, left and right only
# perform outer join on 'Account_Number_External_ID__c'
both_df, left_only_df, right_only_df = Utils.get_df_diffs(account_df, df_to_upload, left_on = 'Account_Number_External_ID__c', right_on = 'Account_Number_External_ID__c', how = 'outer', suffixes = ('_left', '_right'), indicator = True, validate = None)


# isolate the only columns needed to make updates to the data in the dataframe before uploading to the table
both_df = both_df[['Account_Number_External_ID__c', 'NumberOfLocations__c_left', 'NumberOfEmployees_left', "SLASerialNumber__c_left", "SLA__c_left"]]

# rename columns to remove the _left at the end from the join
both_df.rename(columns = {'Account_Number_External_ID__c_left': 'Account_Number_External_ID__c',
                       'NumberOfLocations__c_left' : 'NumberOfLocations__c',
                       'NumberOfEmployees_left' : 'NumberOfEmployees',
                       "SLASerialNumber__c_left" : "SLASerialNumber__c",
                       "SLA__c_left" : "SLA__c"}, inplace = True)

#
for index, row in both_df.iterrows():
    # first four ifs modify number of locations based on number of employees
    # last if modifies sla value
    if int(row['NumberOfEmployees']) < 10:
        # if number of employees is below 10, modify number of locations to 1
        both_df.at[index, 'NumberOfLocations__c'] = "1"
    if int(row['NumberOfEmployees']) > 10 and int(row['NumberOfEmployees']) < 50:
        # if number of employees is between 10 and 50, modify number of locations to 2
        both_df.at[index, 'NumberOfLocations__c'] = "2"
    if int(row['NumberOfEmployees']) > 50 and int(row['NumberOfEmployees']) < 100:
        # if number of employees is between 50 and 100, modify number of locations to 3
        both_df.at[index, 'NumberOfLocations__c'] = "3"
    if int(row['NumberOfEmployees']) > 100:
        # if number of employees is greater than 100, modify number of locations to 4
        both_df.at[index, 'NumberOfLocations__c'] = "5"
    if row["SLASerialNumber__c"][0:1] == "1":
        # if first digit in serial number is a 1, set sla value to platinum
        both_df.at[index, 'SLA__c'] = "Platinum"

# list columns to be updated from dataframe, used to not update other columns unintentionally
columns_to_update = ['SLA__c', 'NumberOfLocations__c']

#set table to update
table_name = 'data_engineering.accounts_test_1'

#table key field
where_column_name = 'Account_Number_External_ID__c'

# # upload the update call of records to mysql
MySQL_Utils.update_rows_in_mysql_table(connection, cursor, both_df, table_name,
                                      columns_to_update, where_column_name)

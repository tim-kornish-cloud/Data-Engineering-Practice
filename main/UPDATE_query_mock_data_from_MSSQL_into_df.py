"""
Author: Timothy Kornish
CreatedDate: August - 16 - 2025
Description: Load a csv of mock data into a pandas dataframe.
             log into a MSSQL table.

"""

import numpy as np
import pandas as pd
import os
from custom_db_utilities import  Custom_MSSQL_Utilities, Custom_Utilities
from credentials import Credentials

#create and instance of the custom salesforce utilities class used to interact with Salesforce
MSSQL_Utils = Custom_MSSQL_Utilities()
# create and instance of the custom  utilities class
Utils = Custom_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

# declare which environment this script will perform operations against,
# can have multiple environments in the same script at the same time
environment = 'localhost'

# number of records to attempted
num_of_records = 10

# starting index to choose records
record_start = 80

# set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# set up fallout ans success path to save files to
# success file path
success_file = dir_path + "\\Output\\INSERT\\SUCCESS_Insert_" + environment + ".csv"
# fallout file path
fallout_file = dir_path + "\\Output\\INSERT\\FALLOUT_Insert_" + environment + ".csv"

# set input path for mock data csv
input_csv_file = dir_path + ".\\MockData\\MOCK_DATA.csv"
# ".\\MockData\\MOCK_DATA_Multi_Data_Types.csv"

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
mock_data_df = pd.read_csv(input_csv_file)

# select only 10 records
df_to_upload = mock_data_df.iloc[record_start:record_start+num_of_records]

# initiate an MS SQL cursor to query with
connection, cursor = MSSQL_Utils.login_to_MSSQL(server = Cred.get_server(), database = Cred.get_database())

# select accounts to match against the csv to not attempt to insert duplicates
select_query = """SELECT TOP (1000) [AccountNumber]
      ,[Name]
      ,[NumberOfEmployees]
      ,[NumberOfLocations__c]
      ,[Phone]
      ,[SLA__c]
      ,[SLASerialNumber__c]
      ,[Account_Number_External_ID__c]
  FROM [Data_Engineering].[dbo].[Accounts_test_1]"""

# accounts in the mssql table shown in the query above
account_df = MSSQL_Utils.query_MSSQL_return_DataFrame(select_query, cursor)

# format the merge column to remove any whitespace
account_df.columns = account_df.columns.str.strip()
mock_data_df.columns = mock_data_df.columns.str.strip()

# format the merge column data types to string before joining
account_df['Account_Number_External_ID__c'] = account_df['Account_Number_External_ID__c'].astype(str)
mock_data_df['Account_Number_External_ID__c'] = mock_data_df['Account_Number_External_ID__c'].astype(str)

# get dataframes for records that exist in both df, left and right only
# perform outer join on 'Account_Number_External_ID__c'
both_df, left_only_df, right_only_df = Utils.get_df_diffs(account_df, mock_data_df, left_on = 'Account_Number_External_ID__c', right_on = 'Account_Number_External_ID__c', how = 'outer', suffixes = ('_left', '_right'), indicator = True, validate = None)


# modify account SLA value to gold
# modify number of locations to 5
account_columns_to_update = ['SLA__c', 'NumberOfLocations__c']
account_values_to_update = ["Gold", "25"]

#set table to update
table_to_update = '[Data_Engineering].[dbo].[Accounts_test_1]'

#table key field
table_UID = 'Account_Number_External_ID__c'

# gerenate sql list of accounts to fit into the where clause of an update
accounts_list = both_df['Account_Number_External_ID__c'].astype(str).tolist()

# below is a sql list as a single string
accounts_to_update_list = Utils.generate_sql_list_from_df_column(both_df, 'Account_Number_External_ID__c', output = 'string')


MSSQL_Utils.update_rows_in_MSSQL_table(connection, cursor, table_to_update,
                                      account_columns_to_update, account_values_to_update,
                                      table_UID, accounts_to_update_list)

"""
keeping here for future example to modify update function

# only keep columns that exist in sql table
# need to remove extra columns and rename existing columns to match the sql table
both_df = both_df[['AccountNumber_left', 'Name_left', 'NumberOfEmployees_left',
                   'NumberOfLocations__c', 'Phone_left', 'SLA__c_left',
                   'SLASerialNumber__c_left', 'Account_Number_External_ID__c']]

# rename dataframe columns to match the MSSQL table column names
accounts_to_update_df = both_df.rename(columns = {"AccountNumber_left" : "AccountNumber",
                          "Name_left" : "Name",
                          "NumberOfEmployees_left" : "NumberOfEmployees",
                          "NumberOfLocations__c" : "NumberOfLocations__c",
                          "Phone_left" : "Phone",
                          "SLA__c_left" : "SLA__c",
                          "SLASerialNumber__c_left" : "SLASerialNumber__c",
                          "Account_Number_External_ID__c" : "Account_Number_External_ID__c"})

# update rows in sql table
MSSQL_Utils.update_rows_in_MSSQL_table(connection, cursor, accounts_to_update_df,
                                      table_to_update,
                                      account_columns_to_update, account_values_to_update,
                                      table_UID, accounts_to_update_list)
"""

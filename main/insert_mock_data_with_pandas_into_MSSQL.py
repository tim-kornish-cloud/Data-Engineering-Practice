"""
Author: Timothy Kornish
CreatedDate: August - 11 - 2025
Description: Load a csv of mock data into a pandas dataframe.
             log into a MSSQL table.
             load records from csv into MSSQL table.
"""

import numpy as np
import pandas as pd
import os
from custom_db_utilities import  MSSQL_Utilities
from credentials import Credentials

# create and instance of the custom salesforce utilities class used to interact with Salesforce
MSSQL_Utils = MSSQL_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

# declare which environment this script will perform operations against,
# can have multiple environments in the same script at the same time
environment = 'localhost'

# number of records to attempted
num_of_records = 10

# starting index to choose records
record_start = 10

# set up directory pathway to load csv data and output fallout and success results to
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

# initiate an MS SQL connection and cursor to query with
connection, cursor = MSSQL_Utils.login_to_mssql(server = Cred.get_server(), database = Cred.get_database())

# list of data types to convert the df columns to fit MSSQL
# need to find a way to parse the df.columns and generate this automatically
# this is a temporaray bandaid being hardcoded, honestly may perform a subset of
# hardcoding these types instead of the entire dataframe
column_types = ('int', 'str', 'int', 'int', 'str', 'str', 'int', 'str')

# mssql table name the dataframe is being inserted into
table_name = 'Accounts_test_1'

# insert subset of the csv  from a dataframe into the mssql table
MSSQL_Utils.insert_dataframe_into_mssql_table(connection, cursor, df_to_upload, table_name, column_types)

# select accounts to match against the csv to not attempt to insert duplicates
select_query = """SELECT TOP (1000) [AccountNumber]
      ,[Name]
      ,[NumberOfEmployees]
      ,[NumberOfLocations__c]
      ,[Phone]
      ,[SLA__c]
      ,[SLASerialNumber__c]
      ,[Account_Number_ExternaL_ID__c]
  FROM [Data_Engineering].[dbo].[Accounts_test_1]"""

# accounts in the mssql table shown in the query above
account_df = MSSQL_Utils.query_mssql_return_dataframe(select_query, cursor)

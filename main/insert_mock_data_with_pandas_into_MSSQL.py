"""
Author: Timothy Kornish
CreatedDate: August - 11 - 2025
Description: Load a csv of mock data into a pandas dataframe.
             log into a MSSQL table.

"""

import numpy as np
import pandas as pd
import os
from custom_db_utilities import  Custom_MSSQL_Utilities
from credentials import Credentials

#create and instance of the custom salesforce utilities class used to interact with Salesforce
MSSQL_Utils = Custom_MSSQL_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

# declare which environment this script will perform operations against,
# can have multiple environments in the same script at the same time
environment = 'localhost'

#number of records to attempted
num_of_records = 10

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

#initiate an MS SQL cursor to query with
connection, cursor = MSSQL_Utils.login_to_MSSQL(server = Cred.get_server(), database = Cred.get_database())

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

#accounts in the mssql table shown in the query above
account_df = MSSQL_Utils.query_MSSQL_return_DataFrame(select_query, cursor)

# list of data types to convert the df columns to fit MSSQL
# need to find a way to parse the df.columns and generate this automatically
# this is a temporaray bandaid being hardcoded
column_types = ('int', 'str', 'int', 'int', 'str', 'str', 'int', 'str')

#insert subset of the csv  from a dataframe into the mssql table
MSSQL_Utils.insert_dataframe_into_MSSQL_table(connection, cursor, df_to_upload, 'Accounts_test_1', column_types)

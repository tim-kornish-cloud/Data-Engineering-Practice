# Author: Timothy Kornish
# CreatedDate: 02/11/2026
# Description: set up a snowflake connection and update a few rows in the accounts test table


# pandas to load mock data
import pandas as pd
# import os for mock data file path specification
import os
# import dml functions from utilities class specific to snowflake
from custom_db_utilities import  Snowflake_Utilities, Custom_Utilities
# retreive stored credentials
from credentials import Credentials

# create and instance of the custom snowflake utilities class used to interact with snowflake DB/tables
Snowflake_Utils = Snowflake_Utilities()
# create instance of Utils class
Utils = Custom_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

# setting database flavor to snowflake
db = "Snowflake"
# setting environment to localhost
environment = "account_test"

# number of records to attempted
num_of_records = 100

# starting index to choose records
record_start = 0

# set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# set up fallout ans success path to save files to
# success file path
success_file = dir_path + "\\Output\\INSERT\\SUCCESS_Insert_" + environment + "_" + db + ".csv"
# fallout file path
fallout_file = dir_path + "\\Output\\INSERT\\FALLOUT_Insert_" + environment + "_" + db + ".csv"

# set input path for mock data csv
input_csv_file = dir_path + ".\\MockData\\MOCK_DATA_Multi_Data_Types.csv"

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
mock_data_df = pd.read_csv(input_csv_file)

# Convert the datetime column to 'DD-MM-YYYY' string format
mock_data_df['CreatedDate'] = pd.to_datetime(mock_data_df['CreatedDate'], errors = 'ignore', dayfirst = True, format = '%m/%d/%Y')
mock_data_df['CreatedDate'] = mock_data_df['CreatedDate'].dt.date
mock_data_df['CreatedDate'] = mock_data_df['CreatedDate'].astype(str)

# select only 10 records
df_to_upload = mock_data_df.iloc[record_start:record_start+num_of_records]

# get username from credentials
username = Cred.get_username(db, environment)
# get password from credentials
password = Cred.get_password(db, environment)
# get account from credentials
account = Cred.get_account(db, environment)
# get db from credentials
database = Cred.get_database(db, environment)
# get warehouse from credentials
warehouse = Cred.get_warehouse(db, environment)
# get schema from credentials
schema = Cred.get_schema(db, environment)

# set up connection to snowflake and create cursor to execute queries with
connection, cursor  = Snowflake_Utils.login_to_snowflake(username, password, account, warehouse, database, schema)

# set query to query all the records that are on the table
accounts_query = "SELECT * FROM accounts_test"

# query the records inserted
account_df = Snowflake_Utils.query_snowflake_return_dataframe(accounts_query, connection, cursor)

# format the merge column to remove any whitespace
account_df.columns = account_df.columns.str.strip()
df_to_upload.columns = df_to_upload.columns.str.strip()

# format the merge column data types to string before joining
account_df['ACCOUNT_NUMBER_EXTERNAL_ID__C'] = account_df['ACCOUNT_NUMBER_EXTERNAL_ID__C'].astype(str)
df_to_upload['Account_Number_External_ID__c'] = df_to_upload['Account_Number_External_ID__c'].astype(str)

# get dataframes for records that exist in both df, left and right only
# perform outer join on 'Account_Number_External_ID__c'
both_df, left_only_df, right_only_df = Utils.get_df_diffs(account_df, df_to_upload, left_on = 'ACCOUNT_NUMBER_EXTERNAL_ID__C', right_on = 'Account_Number_External_ID__c', how = 'outer', suffixes = ('_left', '_right'), indicator = True, validate = None)

# isolate the only columns needed to make updates to the data in the dataframe before uploading to the table
both_df = both_df[['Account_Number_External_ID__c', 'NumberOfLocations__c', 'NumberOfEmployees', "SLASerialNumber__c", "SLA__c"]]

# loop through each row to updddadte the number of employees and sla serial number columns
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
    if str(int(row["SLASerialNumber__c"]))[0:1] == "1":
        # if first digit in serial number is a 1, set sla value to platinum
        both_df.at[index, 'SLA__c'] = "Platinum"

# list columns to be updated from dataframe, used to not update other columns unintentionally
columns_to_update = ['SLA__c', 'NumberOfLocations__c']

#set table to update
table_name = 'accounts_test'

#table key field
where_column_name = 'Account_Number_External_ID__c'

# upload the update call of records to mysql
Snowflake_Utils.update_rows_in_snowflake_table(connection, cursor, both_df, table_name,
                                      columns_to_update, where_column_name)

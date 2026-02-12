# Author: Timothy Kornish
# CreatedDate: 02/11/2026
# Description: set up a snowflake connection and insert a few rows into accounts_test table from a csv


# pandas to load mock data
import pandas as pd
# import os for mock data file path specification
import os
# import dml functions from utilities class specific to snowflake
from custom_db_utilities import  Snowflake_Utilities
# retreive stored credentials
from credentials import Credentials

# create and instance of the custom snowflake utilities class used to interact with snowflake DB/tables
Snowflake_Utils = Snowflake_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

# setting database flavor to snowflake
db = "Snowflake"
# setting environment to localhost
environment = "account_test"

# number of records to attempted
num_of_records = 10

# starting index to choose records
record_start = 30

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

# load records from test table into dataframe using query
Snowflake_Utils.insert_dataframe_into_snowflake_table(connection, cursor, df_to_upload, 'accounts_test')

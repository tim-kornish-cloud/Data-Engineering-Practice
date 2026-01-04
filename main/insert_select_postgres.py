# Author: Timothy Kornish
# CreatedDate: 9/11/2025
# Description: set up a postgres connection and populate database with fake data
# choose how many rows to insert and what row of the csv to start on.
# NOTE: no index out of bound handling on the csv length

# pandas to load mock data
import pandas as pd
# import os for mock data file path specification
import os
# import dml functions from utilities class specific to postgresql
from custom_db_utilities import  Postgres_Utilities
# retreive stored credentials
from credentials import Credentials

# create and instance of the custom postgres utilities class used to interact with postgres DB/tables
Postgres_Utils = Postgres_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

# set up directory pathway to load csv data
dir_path = os.path.dirname(os.path.realpath(__file__))

# set input path for mock data csv
input_csv_file = dir_path + ".\\MockData\\MOCK_DATA_Multi_Data_Types.csv"

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
mock_df = pd.read_csv(input_csv_file)

# setting database flavor to postgres for
database = "Postgres"
# setting environment to localhost
environment = "localhost"

# get username from credentials
username = Cred.get_username(database, environment)
# get password from credentials
password = Cred.get_password(database, environment)
# get host from credentials
host = Cred.get_host(database, environment)
# get database from credentials
database = Cred.get_database(database, environment)
# get port from credentials
port = Cred.get_port()

# set up connection to postgres and create cursor to execute queries with
connection, cursor  = Postgres_Utils.login_to_postgresql(host, database, username, password, port)

# number of records to attempt
num_of_records = 10

# starting index to choose records
record_start = 40

# convert binary data from true/false to 1/0
# will throw warning using .replace(), but don't care for this example
mock_df["IsActive"] = mock_df["IsActive"].replace({True: 1, False: 0})

# select only 10 records
df_to_upload = mock_df.iloc[record_start:record_start+num_of_records]

# hardcoding the types for proper parsing, attempt at auto detect caused issues
# can make this into a dictionary of columns as keys, with types as values
column_types = ('str', 'str', 'int', 'int', 'str', 'str', 'int', 'str', 'str', 'date', 'float')

# postgres table name the dataframe is being inserted into
table_name = 'accounts_test'

# insert rows into table
Postgres_Utils.insert_dataframe_into_postgres_table(connection, cursor, df_to_upload, table_name, column_types)

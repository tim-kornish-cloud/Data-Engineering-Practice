# Author: Timothy Kornish
# CreatedDate: 12/17/2025
# Description: set up a postgres connection and basic select query funneling records into a pandas dataframe


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

# setting database flavor to postgres
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

# basic select SQL to grab all accounts loaded into test table
select_record_sql = """
      SELECT * FROM accounts_test;
      """
# load records from test table into dataframe using query
# will throw warning for pandas only supports SQLAlchemy
transaction_df = Postgres_Utils.query_postgres_return_dataframe(select_record_sql, connection)

# print out the first 5 rows from loading the record into the dataframe
print(transaction_df.head())

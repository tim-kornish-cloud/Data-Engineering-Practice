# Author: Timothy Kornish
# CreatedDate: 9/11/2025
# Description: set up a postgres connection and populate database with fake data

# pandas to load mock data
import pandas as pd
# import os for mock data file path specification
import os
# psycopg2 for connecting postgresql database
import psycopg2
#
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

# set up connection to postgres
connection = psycopg2.connect(
      host = host,
      database = database,
      user = username,
      password = password,
      port = port
)

# create cursor to execute queries with
cursor = connection.cursor()

# set up query to insert a single record into the table
insert_record_sql = """
      INSERT INTO accounts_test(AccountNumber,
                               Name,
                               NumberOfEmployees,
                               NumberofLocations__c,
                               Phone,
                               SLA__c,
                               SLASerialNumber__c,
                               Account_Number_External_ID__c,
                               IsActive,
                               CreatedDate,
                               AmountPaid)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
      """
mock_df["AmountPaid"] = mock_df["AmountPaid"].astype(float)
# Convert True/False to 1/0 using replace
mock_df["IsActive"] = mock_df["IsActive"].replace({True: 1, False: 0})

row_to_insert = 5

# select the values of the first account to insert into the postgres table
first_account = (mock_df["AccountNumber"].astype('str').iloc[row_to_insert],
                 mock_df["Name"].astype('str').iloc[row_to_insert],
                 int(mock_df["NumberOfEmployees"].iloc[row_to_insert]),
                 int(mock_df["NumberofLocations__c"].iloc[row_to_insert]),
                 mock_df["Phone"].astype('str').iloc[row_to_insert],
                 mock_df["SLA__c"].astype('str').iloc[row_to_insert],
                 int(mock_df["SLASerialNumber__c"].iloc[row_to_insert]),
                 mock_df["Account_Number_External_ID__c"].astype('str').iloc[row_to_insert],
                 mock_df["IsActive"].astype('int').astype('str').iloc[row_to_insert],
                 mock_df["CreatedDate"].iloc[row_to_insert],
                 float(mock_df["AmountPaid"].iloc[row_to_insert]))

print(mock_df.head(row_to_insert+ 1))
# upload record to table just created
cursor.execute(insert_record_sql, first_account)

# commit execution
connection.commit()

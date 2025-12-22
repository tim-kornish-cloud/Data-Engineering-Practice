# Author: Timothy Kornish
# CreatedDate: 12/19/2025
# Description: update rows in postgres table from a pandas dataframe.
# perform a join to identify rows in both system, update the row manually,
# then send the update to the corresponding table


# faker for generating fake data
import faker
# psycopg2 for connecting postgresql database
import psycopg2
# import datetime for formatting
from datetime import datetime
# import random generator
import random
# retreive stored credentials
from credentials import Credentials

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
port = Cred.get_port(database, environment)


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

# set update syntax for specifically the amount on a users account
# modify to be more dynamic later once update basic concept passing
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

# accounts in the postgres table shown in the query above
account_df = Postgres_Utils.query_postgres_return_dataframe(select_query, cursor)

# format the merge column to remove any whitespace
account_df.columns = account_df.columns.str.strip()
mock_data_df.columns = mock_data_df.columns.str.strip()

# format the merge column data types to string before joining
account_df['Account_Number_External_ID__c'] = account_df['Account_Number_External_ID__c'].astype(str)
mock_data_df['Account_Number_External_ID__c'] = mock_data_df['Account_Number_External_ID__c'].astype(str)

# get dataframes for records that exist in both df, left and right only
# perform outer join on 'Account_Number_External_ID__c'
both_df, left_only_df, right_only_df = Utils.get_df_diffs(account_df, mock_data_df, left_on = 'Account_Number_External_ID__c', right_on = 'Account_Number_External_ID__c', how = 'outer', suffixes = ('_left', '_right'), indicator = True, validate = None)

#set table to update
table_to_delete = '[Data_Engineering].[dbo].[Accounts_test_1]'

# table key field
table_UID = 'Account_Number_External_ID__c'

# below is a sql list as a single string
accounts_to_delete_list = Utils.generate_sql_list_from_df_column(both_df, 'Account_Number_External_ID__c', output = 'string')

# delete records from table in Postgres database
Postgres_Utils.delete_rows_in_postgres_table(connection, cursor, table_to_delete,
                                      table_UID, accounts_to_delete_list)

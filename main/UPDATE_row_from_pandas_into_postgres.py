# Author: Timothy Kornish
# CreatedDate: 12/19/2025
# Description: update rows in postgres table from a pandas dataframe.
# perform a join to identify rows in both system, update the row manually,
# then send the update to the corresponding table


# pandas to load mock data
import pandas as pd
# import os for mock data file path specification
import os
# psycopg2 for connecting postgresql database
import psycopg2
#
from custom_db_utilities import  Postgres_Utilities, Custom_Utilities
# retreive stored credentials
from credentials import Credentials

# create and instance of the custom postgres utilities class used to interact with postgres DB/tables
Postgres_Utils = Postgres_Utilities()
# create and instance of the custom  utilities class
Utils = Custom_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

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

# set update syntax for specifically the amount on a users account
# modify to be more dynamic later once update basic concept passing
# select accounts to match against the csv to not attempt to insert duplicates
select_query = """SELECT accountnumber,
                         "name",
                         numberofemployees,
                         numberoflocations__c,
                         phone,
                         sla__c,
                         slaserialnumber__c,
                         account_number_external_id__c,
                         isactive,
                         createddate,
                         amountpaid
                  FROM accounts_test;"""

# accounts in the postgres table shown in the query above
account_df = pd.read_sql(select_query, connection)

print(account_df.head())
# modify account SLA value to gold
# modify number of locations to 5
account_columns_to_update = ['sla__c', 'numberoflocations__c']
account_values_to_update = ["Gold", "25"]

#set table to update
table_to_update = 'accounts_test'

#table key field
table_UID = 'account_number_external_id__c'

# below is a sql list as a single string
accounts_to_update_list = Utils.generate_sql_list_from_df_column(account_df, 'account_number_external_id__c', output = 'string')
print(accounts_to_update_list)
# upload the update sql call to Postgres database
Postgres_Utils.update_rows_in_postgres_table(connection, cursor, table_to_update,
                                      account_columns_to_update, account_values_to_update,
                                      table_UID, accounts_to_update_list)

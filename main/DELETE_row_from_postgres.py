# Author: Timothy Kornish
# CreatedDate: 12/19/2025
# Description: update rows in postgres table from a pandas dataframe.


# pandas to load mock data
import pandas as pd
# import os for mock data file path specification
import os
# import dml functions from utilities class specific to postgresql
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

# set up connection to postgres and create cursor to execute queries with
connection, cursor  = Postgres_Utils.login_to_postgresql(host, database, username, password, port)

# query accounts to select for deletion, delete all accounts on table
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
                  FROM accounts_test"""

# accounts in the postgres table shown in the query above
account_df = pd.read_sql(select_query, connection)

#set table to update
table_to_delete = 'Accounts_test'

# table primary key field
table_UID = 'account_number_external_id__c'

# below is a sql list as a single string
accounts_to_delete_list = Utils.generate_sql_list_from_df_column(account_df, 'account_number_external_id__c', output = 'string')

# delete records from table in Postgres database
Postgres_Utils.delete_rows_in_postgres_table(connection, cursor, table_to_delete, table_UID, accounts_to_delete_list)

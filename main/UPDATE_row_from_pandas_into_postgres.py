# Author: Timothy Kornish
# CreatedDate: 12/19/2025
# Description: update rows in postgres table from a pandas dataframe.
# perform a join to identify rows in both system, update the row manually,
# then send the update to the corresponding table


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

# select sql for all accounts to update their sla and number of location values
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
account_df = Postgres_Utils.query_postgres_return_dataframe(select_query, connection)

# modify account SLA value to gold
account_df['sla__c'] = "Gold"
# modify number of locations to 25
account_df['numberoflocations__c'] = "15"

#list all columns to be updated in this call
account_columns_to_update = ['sla__c', 'numberoflocations__c']

# set table to update
table_name = 'accounts_test'

# table key field
table_uid = 'account_number_external_id__c'

# upload the update sql call to Postgres database
Postgres_Utils.update_rows_in_postgres_table(connection, cursor, account_df, table_name,
                                             account_columns_to_update, table_uid)

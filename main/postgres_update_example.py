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

# loop through the dataframe and modify the sla and number of employee fields
# modify number of locations based on number of employees
# modify the sla value based on first digit in sla serial number column
for index, row in account_df.iterrows():
    # first four ifs modify number of locations based on number of employees
    # last if modifies sla value
    if int(row['numberofemployees']) < 10:
        # if number of employees is below 10, modify number of locations to 1
        account_df.at[index, 'numberoflocations__c'] = "1"
    if int(row['numberofemployees']) > 10 and int(row['numberofemployees']) < 50:
        # if number of employees is between 10 and 50, modify number of locations to 2
        account_df.at[index, 'numberoflocations__c'] = "2"
    if int(row['numberofemployees']) > 50 and int(row['numberofemployees']) < 100:
        # if number of employees is between 50 and 100, modify number of locations to 3
        account_df.at[index, 'numberoflocations__c'] = "3"
    if int(row['numberofemployees']) > 100:
        # if number of employees is greater than 100, modify number of locations to 4
        account_df.at[index, 'numberoflocations__c'] = "5"
    if row["slaserialnumber__c"][0:1] == "1":
        # if first digit in serial number is a 1, set sla value to platinum
        account_df.at[index, 'sla__c'] = "Platinum"

#list all columns to be updated in this call
account_columns_to_update = ['sla__c', 'numberoflocations__c']

# set table to update
table_name = 'accounts_test'

# table key field
table_uid = 'account_number_external_id__c'

# upload the update sql call to Postgres database
Postgres_Utils.update_rows_in_postgres_table(connection, cursor, account_df, table_name,
                                             account_columns_to_update, table_uid)

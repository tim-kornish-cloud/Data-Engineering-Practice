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

from psycopg2.extras import execute_values

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
account_df['numberoflocations__c'] = "25"

#list all columns to be updated in this call
account_columns_to_update = ['sla__c', 'numberoflocations__c']

# set table to update
table_name = 'accounts_test'

# table key field
table_uid = 'account_number_external_id__c'
# table key field in dataframe, in case there is a mismatch
df_uid = 'account_number_external_id__c'

# below is a sql list as a single string
accounts_to_update_list = Utils.generate_sql_list_from_df_column(account_df, 'account_number_external_id__c', output = 'string')

# create beginning of update, add table name
sql_update = f"UPDATE {table_name} SET "
col_list = ""
df_col_list = [df_uid]
# loop through fields on df
for col in account_columns_to_update:
    # assume table and dataframe column names are identical, rename df column names if mismatch
    sql_update = sql_update + f"{col} = data.{col}, "
    col_list = col_list + col + ", "
    df_col_list.extend([col])
sql_update = sql_update[:-2]
col_list = col_list[:-2]
sql_update = sql_update + f" FROM (VALUES %s) AS data ({df_uid}, {col_list}) WHERE {table_name}.{table_uid} = data.{df_uid}"

print(sql_update)
print(df_col_list)
df_to_update = account_df[df_col_list]
print(df_to_update.head())

# convert the rows in the dataframe into tuples
data = [tuple(x) for x in df_to_update.values]

# execute insert of records
execute_values(cursor, sql_update, data)

# commit the sql statement
connection.commit()

# # upload the update sql call to Postgres database
# Postgres_Utils.update_rows_in_postgres_table(connection, cursor, table_to_update,
#                                       account_columns_to_update, account_values_to_update,
#                                       table_UID, accounts_to_update_list)

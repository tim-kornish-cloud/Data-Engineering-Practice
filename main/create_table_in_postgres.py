# Author: Timothy Kornish
# CreatedDate: 12/22/2025
# Description: set up a postgres connection and create table to
# match format of mock data used in other dbms examples


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

# set up query to create a table in postgresql db
create_table_sql = """
      CREATE TABLE IF NOT EXISTS transactions (
          AccountNumber VARCHAR(255) PRIMARY KEY,
          Name VARCHAR(255),
          NumberOfEmployees VARCHAR(255),
          NumberofLocations__c VARCHAR(255),
          Phone VARCHAR(255),
          SLA__c VARCHAR(255),
          SLASerialNumber__c VARCHAR(255),
          Account_Number_External_ID__c VARCHAR(255),
          IsActive VARCHAR(255),
          CreatedDate VARCHAR(255),
          AmountPaid VARCHAR(255)
      )
      """

# execute query to create table
execute_sql(connection, cursor, create_table_sql, False)

# close the cursor before commiting with connection
cursor.close()

# commit execution
connection.commit()

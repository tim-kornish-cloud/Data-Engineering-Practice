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
delete_record_sql = """
DELETE FROM transactions
WHERE userId = %s
"""

# query the records from the postgres table, load into a DataFrames
# get userId for record to be deleted and load into variable user_id

# execute the update against the dataframe values
cursor.execute(delete_record_sql,  user_id)

connection.commit()

# Author: Timothy Kornish
# CreatedDate: 12/17/2025
# Description: set up a postgres connection and basic select query funneling records into a pandas dataframe


# faker for generating fake data
import faker
# psycopg2 for connecting postgresql database
import psycopg2
# import datetime for formatting
from datetime import datetime
# import random generator
import random

# set up connection to postgres
connection = psycopg2.connect(
      host="localhost",
      database="financial_db",
      user="postgres",
      password="postgres",
      port=5432
  )

# create cursor to execute queries with
cursor = connection.cursor()

select_record_sql = """
      SELECT * INTO transactions LIMIT 1;
      """

transaction_df = pd.read_sql(select_record_sql, connection)

print(transaction_df.head())

# Author: Timothy Kornish
# CreatedDate: 9/11/2025
# Description: set up a postgres connection and populate database with fake data

# faker for generating fake data
import faker
# psycopg2 for connecting postgresql database
import psycopg2
# import datetime for formatting
from datetime import datetime
# import random generator
import random


def generate_transaction(fake, user):
  """
  Description: generate a row of fake data to load into a postgres table
  Parameters:

  faker     - instance of faker class to generate fake data
  user      - fake.simple_profile(), simple profile of fake user data

  Return:   - dictionary, a record to load into postgresql table
  """
  return {
    "transactionId": fake.uuid4(),
    "userId": user["username"],
    "timestamp": datetime.utcnow().timestamp(),
    "amount": round(random.uniform(10, 1000), 2),
    "currency": random.choice(["USD", "GBP"]),
    "city": fake.city(),
    "country": fake.country(),
    "merchantName": fake.company(),
    "paymentMethod": random.choice(["credit_card", "debit_card", "online_transfer"]),
    "ipAddress": fake.ipv4(),
    "voucherCode": random.choice(["", "DISCOUNT10", ""]),
    "affiliateId": fake.uuid4()
  }


# set up faker instance to generate data
fake_data = faker.Faker()
# set up user profile with fake data
user = fake_data.simple_profile()

# set up query to create a table in postgresql db
create_table_sql = """
      CREATE TABLE IF NOT EXISTS transactions (
          transaction_id VARCHAR(255) PRIMARY KEY,
          user_id VARCHAR(255),
          timestamp TIMESTAMP,
          amount DECIMAL,
          currency VARCHAR(255),
          city VARCHAR(255),
          country VARCHAR(255),
          merchant_name VARCHAR(255),
          payment_method VARCHAR(255),
          ip_address VARCHAR(255),
          voucher_code VARCHAR(255),
          affiliateId VARCHAR(255)
      )
      """
# set up query to insert a single record into the table
insert_record_sql = """
      INSERT INTO transactions(transaction_id, user_id, timestamp, amount, currency, city, country, merchant_name, payment_method,
      ip_address, affiliateId, voucher_code)
      VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
      """
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

# execute query to create table
execute_sql(connection, cursor, create_table_sql, False)

# get record to insert
transaction = generate_transaction(fake_data, user)

# create list of values to insert with query
transaction_list = (transaction["transactionId"],
                    transaction["userId"],
                    datetime.fromtimestamp(transaction["timestamp"]).strftime("%Y-%m-%d %H:%M:%S"),
                    transaction["amount"],
                    transaction["currency"],
                    transaction["city"],
                    transaction["country"],
                    transaction["merchantName"],
                    transaction["paymentMethod"],
                    transaction["ipAddress"],
                    transaction["affiliateId"],
                    transaction["voucherCode"])

# upload record to table just created
cursor.execute(insert_record_sql, transaction_list)

# close the cursor before commiting with connection
cursor.close()

# commit execution
connection.commit()

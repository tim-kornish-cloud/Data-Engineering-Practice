# Author: Timothy Kornish
# CreatedDate: 9/11/2025
# Description: set up a postgres connection and populate database with fake data

# pandas to load mock data
import pandas as pd
# import os for mock data file path specification
import os
# psycopg2 for connecting postgresql database
import psycopg2
#
from custom_db_utilities import  Postgres_Utilities
# retreive stored credentials
from credentials import Credentials

# create and instance of the custom postgres utilities class used to interact with postgres DB/tables
Postgres_Utils = Postgres_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

# set up directory pathway to load csv data
dir_path = os.path.dirname(os.path.realpath(__file__))

# set input path for mock data csv
input_csv_file = dir_path + ".\\MockData\\MOCK_DATA_Multi_Data_Types.csv"

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
mock_df = pd.read_csv(input_csv_file)

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
port = 5432 #Cred.get_port(database, environment)

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

# set up query to insert a single record into the table
insert_record_sql = """
      INSERT INTO accounts_test(AccountNumber,
                               Name,
                               NumberOfEmployees,
                               NumberofLocations__c,
                               Phone,
                               SLA__c,
                               SLASerialNumber__c,
                               Account_Number_External_ID__c,
                               IsActive,
                               CreatedDate,
                               AmountPaid)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
      """
mock_df["AmountPaid"] = mock_df["AmountPaid"].astype(float)
# Convert True/False to 1/0 using replace
mock_df["IsActive"] = mock_df["IsActive"].replace({True: 1, False: 0})

row_to_insert = 4

# select the values of the first account to insert into the postgres table
first_account = (mock_df["AccountNumber"].astype('str').iloc[row_to_insert],
                 mock_df["Name"].astype('str').iloc[row_to_insert],
                 int(mock_df["NumberOfEmployees"].iloc[row_to_insert]),
                 int(mock_df["NumberofLocations__c"].iloc[row_to_insert]),
                 mock_df["Phone"].astype('str').iloc[row_to_insert],
                 mock_df["SLA__c"].astype('str').iloc[row_to_insert],
                 int(mock_df["SLASerialNumber__c"].iloc[row_to_insert]),
                 mock_df["Account_Number_External_ID__c"].astype('str').iloc[row_to_insert],
                 mock_df["IsActive"].astype('int').astype('str').iloc[row_to_insert],
                 mock_df["CreatedDate"].iloc[row_to_insert],
                 float(mock_df["AmountPaid"].iloc[row_to_insert]))

# first_account = (
#     "1",
#     "Berni",
#     19,
#     50,
#     "801-507-7120",
#     "gold",
#     242272447,
#     "7d578aec-9b1d-4cdc-9dc3-f87fd1ced883",
#     '0',
#     "4/9/2025",
#     30.65
# )

print(mock_df.head(row_to_insert+ 1))
# upload record to table just created
cursor.execute(insert_record_sql, first_account)

# commit execution
connection.commit()


# # set up faker instance to generate data
# fake_data = faker.Faker()
# # set up user profile with fake data
# user = fake_data.simple_profile()
# def generate_transaction(fake, user):
#   """
#   Description: generate a row of fake data to load into a postgres table
#   Parameters:
#
#   faker     - instance of faker class to generate fake data
#   user      - fake.simple_profile(), simple profile of fake user data
#
#   Return:   - dictionary, a record to load into postgresql table
#   """
#   return {
#     "transactionId": fake.uuid4(),
#     "userId": user["username"],
#     "timestamp": datetime.utcnow().timestamp(),
#     "amount": round(random.uniform(10, 1000), 2),
#     "currency": random.choice(["USD", "GBP"]),
#     "city": fake.city(),
#     "country": fake.country(),
#     "merchantName": fake.company(),
#     "paymentMethod": random.choice(["credit_card", "debit_card", "online_transfer"]),
#     "ipAddress": fake.ipv4(),
#     "voucherCode": random.choice(["", "DISCOUNT10", ""]),
#     "affiliateId": fake.uuid4()
#   }

# # get record to insert
# transaction = generate_transaction(fake_data, user)
#
# # create list of values to insert with query
# transaction_list = (transaction["transactionId"],
#                     transaction["userId"],
#                     datetime.fromtimestamp(transaction["timestamp"]).strftime("%Y-%m-%d %H:%M:%S"),
#                     transaction["amount"],
#                     transaction["currency"],
#                     transaction["city"],
#                     transaction["country"],
#                     transaction["merchantName"],
#                     transaction["paymentMethod"],
#                     transaction["ipAddress"],
#                     transaction["affiliateId"],
#                     transaction["voucherCode"])

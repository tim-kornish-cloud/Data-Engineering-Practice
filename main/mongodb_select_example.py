"""
Author: Timothy Kornish
CreatedDate: August - 22 - 2025
Description: Load records from a table in mongodb into a pandas dataframe
"""
import numpy as np
import pandas as pd
import os
from custom_db_utilities import  MongoDB_Utilities
from credentials import Credentials

# create and instance of the custom salesforce utilities class used to interact with Salesforce
MongoDB_Utils = MongoDB_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

# declare which environment this script will perform operations against,
# can have multiple environments in the same script at the same time
environment = "localhost"
# set the database value to load credentials and log success/fallout
database = "MongoDB"

# number of records to attempted
num_of_records = 10

# starting index to choose records
record_start = 5

# set up directory pathway to load csv data
dir_path = os.path.dirname(os.path.realpath(__file__))

# set input path for mock data csv
input_csv_file = dir_path + ".\\MockData\\MOCK_DATA_Multi_Data_Types.csv"

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
mock_df = pd.read_csv(input_csv_file)

# select only 10 records
df_to_upload = mock_df.iloc[record_start:record_start+num_of_records]

# retrieve mongodb uri
mongodb_uri = Cred.get_uri(dbms = database, env = environment)

# create client connection with mongodb
client = MongoDB_Utils.create_mongo_client(uri = mongodb_uri)

# mongodb database name
mongo_db_name = "data_engineering"
# create database variable
mongo_db = client[mongo_db_name]

# mongodb database collection name
mongo_collection_name = "accounts_test_2"
# create collection variable
mongo_collection = mongo_db[mongo_collection_name]

# query a subset of records using the field SLC__c
query_field = "SLA__c"
# query a subset of records where the field SLC__c = "gold"
query_value = "gold"

# query records from the mongodb collection with the above field value pair
accounts_df = MongoDB_Utils.query_dataframe_from_mongodb_collection(client, mongo_db, mongo_collection, query_field, query_value, close_connection = False)

# print out the number of records queried
print(len(accounts_df))
# print out the first 5 queried records of the entire collection
print(accounts_df.head())

# query all records from the mongodb collection, leaving the field value pair set to none
accounts_df = MongoDB_Utils.query_dataframe_from_mongodb_collection(client, mongo_db, mongo_collection)

# print out the number of records queried
print(len(accounts_df))
# print out the first 5 queried records of the entire collection
print(accounts_df.head())

"""
Author: Timothy Kornish
CreatedDate: August - 23 - 2025
Description: Load records from a csv into a pandas dataframe and update
             the corresponding records in a mongodb database collection.
             Before commiting the update, change two columns in this script:
             SLA__c = "platinum"
             IsActive = True
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
record_start = 0

# set up directory pathway to load csv data
dir_path = os.path.dirname(os.path.realpath(__file__))

# set input path for mock data csv
input_csv_file = dir_path + ".\\MockData\\MOCK_DATA_Multi_Data_Types.csv"

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
mock_df = pd.read_csv(input_csv_file)

# select only 10 records
df_to_update = mock_df.iloc[record_start:record_start+num_of_records]

# retrieve mongodb uri
mongodb_uri = Cred.get_uri(dbms = database, env = environment)

# create client connection with mongodb
client = MongoDB_Utils.create_mongo_client(uri = mongodb_uri)

# update all records in the dataframe to have SLA__c = "platinum"
record_field_override = "SLA__c"
record_value_override = "platinum"

# update the column SLA__c
df_to_update.loc[:,record_field_override] = record_value_override

# update all records in the dataframe to have IsActive = True
record_field_override = "IsActive"
record_value_override = True

# update the column IsActive
df_to_update.loc[:,record_field_override] = record_value_override

# mongodb database name
mongo_db_name = "data_engineering"
# create database variable
mongo_db = client[mongo_db_name]

# mongodb database collection name
mongo_collection_name = "accounts_test_2"
# create collection variable
mongo_collection = mongo_db[mongo_collection_name]

# results = MongoDB_Utils.insert_dataframe_into_mongodb_collection(record_to_insert)
results = MongoDB_Utils.update_dataframe_in_mongodb_collection(df_to_update,
                                                               client,
                                                               mongo_db,
                                                               mongo_collection,
                                                               field = "Account_Number_External_ID__c")

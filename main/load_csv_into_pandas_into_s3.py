"""
Author: Timothy Kornish
CreatedDate: August - 21 - 2025
Description: Load a csv of mock data into a pandas dataframe.
             log into aws with boto3 and awswrangler and load
             the csv to the s3 bucket from the dataframe.

"""

import numpy as np
import pandas as pd
import os
import boto3
import awswrangler
from custom_db_utilities import  EC2_S3_Utilities
from credentials import Credentials

# create and instance of the custom salesforce utilities class used to interact with Salesforce
AWS_Utils = EC2_S3_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

# declare which environment this script will perform operations against,
# can have multiple environments in the same script at the same time
environment = 'AWS_CLI'
# set the database value to load credentials and log success/fallout
database = "AWS"

# number of records to attempted
num_of_records = 10

# starting index to choose records
record_start = 5

# set up directory pathway to load csv data
dir_path = os.path.dirname(os.path.realpath(__file__))

# set input path for mock data csv
input_csv_file = dir_path + ".\\MockData\\MOCK_DATA.csv"

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
mock_df = pd.read_csv(input_csv_file)

# select only 10 records
df_to_upload = mock_df.iloc[record_start:record_start+num_of_records]

# set s3 bucket name to upload csv to
s3_bucket_name = 'pandas-interface-bucket'
# set name of the csv file being uploaded to s3 bucket
filename = 'MOCK_DATA_Multi_Data_Types_subset.csv'

# upload dataframe to s3 bucket
AWS_Utils.upload_dataframe_to_s3(df_to_upload, s3_bucket_name, filename)

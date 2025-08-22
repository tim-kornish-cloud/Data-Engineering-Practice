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
SF_Utils = EC2_S3_Utilities()
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

# set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# set up fallout ans success path to save files to
# success file path
success_file = dir_path + "\\Output\\DELETE\\SUCCESS_Delete_" + environment + "_" + database + ".csv"
# fallout file path
fallout_file = dir_path + "\\Output\\DELETE\\FALLOUT_Delete_" + environment + "_" + database + ".csv"

# set input path for mock data csv
input_csv_file = dir_path + ".\\MockData\\MOCK_DATA.csv"

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
mock_df = pd.read_csv(input_csv_file)

# select only 10 records
df_to_upload = mock_df.iloc[record_start:record_start+num_of_records]

#log into boto3 with Credentials
s3_client = boto3.client(service_name = 's3',
                         region_name = Cred.get_region(dbms = database, env = environment),
                         aws_access_key_id = Cred.get_access_key_id(dbms = database, env = environment),
                         aws_secret_access_key = Cred.get_secret_access_key(dbms = database, env = environment))

# s3 bucket name to upload csv to
s3_bucket_name = 'pandas-interface-bucket'
# name of the csv file being uploaded to s3 bucket
filename = 'MOCK_DATA_Multi_Data_Types_subset.csv'
# create path string to upload csv
s3_path = f"s3://{s3_bucket_name}/{filename}"
# upload csv to s3 path
df_to_upload.to_csv(s3_path, index = False)
# print out showing success of upload
print(f"DataFrame successfully saved to {s3_path}")






# keeping here as first example code to modify from

# import boto3
# import pandas as pd
# from io import BytesIO
#
# csv_buffer = BytesIO()
# df.to_csv(csv_buffer, index=False)
# csv_buffer.seek(0) # Rewind the buffer to the beginning
#
# s3_client = boto3.client(service_name = 's3', region_name=None,aws_access_key_id=None, aws_secret_access_key=None)
# bucket_name = 'pandas-interface-bucket' # Replace with your S3 bucket name
# filename = 'MOCK_DATA_Multi_Data_Types.csv' # Desired key (path and filename) in S3
#
# #
# #s3_client.upload_fileobj(csv_buffer, bucket_name, filename)
# #print(f"DataFrame uploaded to s3://{bucket_name}/{filename}")
#
#
# df.to_csv('s3://pandas-interface-bucket/MOCK_DATA_Multi_Data_Types.csv', index=False)

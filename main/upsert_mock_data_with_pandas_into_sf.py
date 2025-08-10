"""
Author: Timothy Kornish
CreatedDate: August - 10 - 2025
Description: Log into Salesforce and upsert a specific set of accounts
             based on query criteria

"""

import numpy as np
import pandas as pd
import os
from simple_salesforce import Salesforce
from custom_sf_utilities import  Custom_SF_Utilities, Custom_Utilities
from credentials import Credentials

#create and instance of the custom salesforce utilities class used to interact with Salesforce
SF_Utils = Custom_SF_Utilities()
#create and instance of the custom utilities class used to format and modify dataframe data
Utils = Custom_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

# declare which environment this script will perform operations against,
# can have multiple environments in the same script at the same time
environment = 'Dev'

#number of records to attempted
num_of_records = 10

#starting index to choose records
record_start = 30

# query string to select records from salesforce
# before uploading with a delete  DML operation
account_query = "SELECT Id FROM Account WHERE CreatedBy.Name = 'Timothy Kornish'"

#set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# set up fallout ans success path to save files to
# success file path
success_file = dir_path + "\\Output\\UPSERT\\SUCCESS_Upsert_" + environment + ".csv"
# fallout file path
fallout_file = dir_path + "\\Output\\UPSERT\\FALLOUT_Upsert_" + environment + ".csv"


# load credentials for Salesforce and the Dev environement
# I use this method instead of a hardcoding credentials and instead of a
# properties or txt file due to parsing issues
# The credentials are stored as strings in a dictionary attribute of a class

# get username from credentials
username = Cred.get_username("Salesforce", environment)
# get password from credentials
password = Cred.get_password("Salesforce", environment)
# get login token from credentials
token = Cred.get_token("Salesforce", environment)

# create a instance of simple_salesforce to query and perform operations against salesforce with
sf = SF_Utils.login_to_salesForce(username, password, token)

# select only 10 records
df_to_upload = mock_df.iloc[record_start:record_start+num_of_records]

# select only 10 records
df_to_upsert = mock_df.iloc[record_start:record_start+num_of_records]

# upload the records to salesforce
SF_Utils.upload_records_to_salesforce(sf, df_to_upsert, 'Account', 'upsert', success_file, fallout_file)

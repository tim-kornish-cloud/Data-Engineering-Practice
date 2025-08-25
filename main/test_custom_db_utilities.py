"""
Author: Timothy Kornish
CreatedDate: August - 25 - 2025
Description: test class for all database operations from custom_db_utilities.py

"""

import unittest
from unittest import mock
from unittest.mock import patch
from pandas.testing import assert_frame_equal, assert_series_equal
import pandas as pd
import numpy as np
import os
from credentials import Credentials
from custom_db_utilities import  Salesforce_Utilities, EC2_S3_Utilities

# create instance of credentials class where creds are stored to load into the test functions
Cred = Credentials()

# set up directory pathway to load csv data
dir_path = os.path.dirname(os.path.realpath(__file__))

class TestSalesforce_Utilities(unittest.TestCase):
    """ Tests for Salesforce_Utilities"""

    def setUp(self):
        """
        Description: create mock records to query/insert/update/upsert/delete
                     against each database

        """
        self.sf_database = "Salesforce"
        self.sf_environment = "Dev"
        self.records = [
            {
                "AccountNumber" : 1,
                "Name" : "Giorgio",
                "NumberOfEmployees" : 95,
                "NumberOfLocations__c" : 43,
                "Phone" : "767-695-2388",
                "SLA__c" : "Gold",
                "SLASerialNumber__c" : "111920516",
                "Account_Number_ExternaL_ID__c" : "978151df-ec7b-4e3b-8109-02a139ddb7f0"
            },
            {
                "AccountNumber" : 2,
                "Name" : "Germayne",
                "NumberOfEmployees" : 38,
                "NumberOfLocations__c" : 26,
                "Phone" : "569-827-8112",
                "SLA__c" : "Gold",
                "SLASerialNumber__c" : "122105744",
                "Account_Number_ExternaL_ID__c" : "03441cbe-21aa-442b-9fdf-315f6a102159"
            },
            {
                "AccountNumber" : 3,
                "Name" : "Marielle",
                "NumberOfEmployees" : 50,
                "NumberOfLocations__c" : 42,
                "Phone" : "935-636-7107",
                "SLA__c" : "bronze",
                "SLASerialNumber__c" : "101205681",
                "Account_Number_ExternaL_ID__c" : "cbaef138-60b5-4560-898f-04de85347cc2"
            }
        ]
        self.credentials = Cred = Credentials(),
        # set input path for mock data csv
        self.input_csv_file = dir_path + ".\\MockData\\MOCK_DATA.csv"
        }

    def test_successful_salesforce_login_insert_then_query(self):
        """Description: This test performs the following operations

        1) create login to salesforce
        2) load a dictionary record into a dataframe of length 10
        3) upload the dataframe to Salesforce
        4) query the inserted record and load results into a new DataFrame
        5) pandas testing assert dataframes are equal (original dataframe, queried dataframe)

        This test covers the functions from Salesforce_Utilities:
            - login_to_salesForce
            - query_salesforce
            - load_query_into_dataframe
            - load_query_with_lookups_into_dataframe
            - format_date_to_salesforce_date
            - reformat_dataframe_to_salesforce_records
            - upload_dataframe_to_salesforce
        missing:
            - flatten_lookup_fieldname_hierarchy

        DML operations included: INSERT, SELECT
        """

    def test_successful_salesforce_login_update_then_query(self):
        """Description: This test performs the following operations

        1) create login to salesforce
        2) load a dictionary record into a dataframe of length 5
        3) upload the dataframe to Salesforce to update all test records
        4) query the updated record and load results into a new DataFrame
        5) pandas testing assert dataframes are equal (original dataframe, queried dataframe)

        This test covers the functions from Salesforce_Utilities:
            - login_to_salesForce
            - query_salesforce
            - load_query_into_dataframe
            - load_query_with_lookups_into_dataframe
            - format_date_to_salesforce_date
            - reformat_dataframe_to_salesforce_records
            - upload_dataframe_to_salesforce
        missing:
            - flatten_lookup_fieldname_hierarchy

        DML operations included: UPDATE, SELECT
        """
    def test_successful_salesforce_login_upsert_then_query(self):
        """Description: This test performs the following operations

        1) create login to salesforce
        2) load a dictionary record into a dataframe of length 10
        3) choose 5 that are already loaded and 5 new.
        4) upload the dataframe to Salesforce to upsert all test records
        5) query the record and load results into a new DataFrame
        6) pandas testing assert dataframes are equal (original dataframe, queried dataframe)

        This test covers the functions from Salesforce_Utilities:
            - login_to_salesForce
            - query_salesforce
            - load_query_into_dataframe
            - load_query_with_lookups_into_dataframe
            - format_date_to_salesforce_date
            - reformat_dataframe_to_salesforce_records
            - upload_dataframe_to_salesforce
        missing:
            - flatten_lookup_fieldname_hierarchy

        DML operations included: UPSERT, SELECT
        """

    def test_successful_salesforce_login_delete_then_query(self):
        """Description: This test performs the following operations

        1) create login to salesforce
        2) load a dictionary record into a dataframe of length ...
        3) upload the dataframe to Salesforce to delete all test records in Salesforce
        4) query the record and load results into a new DataFrame
        5) pandas testing assert dataframes are equal (original dataframe, queried dataframe)

        This test covers the functions from Salesforce_Utilities:
            - login_to_salesForce
            - query_salesforce
            - load_query_into_dataframe
            - load_query_with_lookups_into_dataframe
            - format_date_to_salesforce_date
            - reformat_dataframe_to_salesforce_records
            - upload_dataframe_to_salesforce
        missing:
            - flatten_lookup_fieldname_hierarchy

        DML operations included: DELETE, SELECT
        """

if __name__ == '__main__':
    unittest.main()

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
from custom_db_utilities import  Salesforce_Utilities, MSSQL_Utilities, MySQL_Utilities, EC2_S3_Utilities, MongoDB_Utilities, Custom_Utilities

# set up directory pathway to load csv data
dir_path = os.path.dirname(os.path.realpath(__file__))

class TestSalesforce_Utilities(unittest.TestCase):
    """ Tests for Salesforce_Utilities"""

    def setUp(self):
        """
        Description: create mock records to query/insert/update/upsert/delete
                     against each database

        """
        # create list of dictionary of accounts to be used for testing along with csv data
        self.account_records = [
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
        # create instance of credentials class where creds are stored to load into the test functions
        self.credentials = Credentials()
        # create instance of custom utilities class to modifying datasets and comparing
        self.utils = Custom_Utilities()
        # create instance of salesforce utility class
        self.sf_utils       = Salesforce_Utilities()
        # create instance of MSSQL utility class
        self.mssql_utils    = MSSQL_Utilities()
        # create instance of MySQL utility class
        self.mysql_utils    = MySQL_Utilities()
        # create instance of EC2_S3 utility class
        self.s3_utils       = EC2_S3_Utilities()
        # create instance of MongoDB utility class
        self.mongosb_utils  = MongoDB_Utilities()

        # set input path for mock data csv
        self.input_csv_file = dir_path + ".\\MockData\\MOCK_DATA.csv"
        # read the mock data csv into a pandas dataframe at the beginning of every test
        self.mock_data_df = pd.read_csv(self.input_csv_file)

    def test_successful_salesforce_login_insert_then_query(self):
        """Description: This test performs the following operations

        1) create login to salesforce
        2) load a mock csv data into a dataframe and keep a slice of length 10
        3) upload the dataframe to Salesforce to insert 10 records
        4) query the inserted records and load results into a new DataFrame
        5) clean up environment, delete inserted records
        6) pandas testing assert dataframes are equal (original dataframe, queried dataframe)

        This test covers the functions from
        Salesforce_Utilities:
            - login_to_salesForce
            - query_salesforce
            - load_query_into_dataframe
            - load_query_with_lookups_into_dataframe
            - format_date_to_salesforce_date
            - reformat_dataframe_to_salesforce_records
            - upload_dataframe_to_salesforce
        Custom_Utilities:
            - get_slice_of_dataframe
            - format_columns_dtypes

        DML operations included: INSERT, SELECT
        """
        #-----------------------------------------------------------------------
        # 1) create login to salesforce
        #-----------------------------------------------------------------------
        # get username from credentials
        username = self.credentials.get_username("Salesforce", "Dev")
        # get password from credentials
        password = self.credentials.get_password("Salesforce", "Dev")
        # get login token from credentials
        token = self.credentials.get_token("Salesforce", "Dev")
        # create a instance of simple_salesforce to query and perform operations against salesforce with
        sf = self.sf_utils.login_to_salesForce(username, password, token)

        #-----------------------------------------------------------------------
        # 2) load a mock csv data into a dataframe and keep a slice of length 10
        #-----------------------------------------------------------------------
        # set record start index
        starting_index = 0
        # set number of records to keep
        number_of_records = 10
        # select only 10 records
        df_to_upload = self.utils.get_slice_of_dataframe(self.mock_data_df, starting_index, number_of_records)

        #-----------------------------------------------------------------------
        # 3) upload the dataframe to Salesforce to insert 10 records
        #-----------------------------------------------------------------------
        # upload the records to salesforce
        self.sf_utils.upload_dataframe_to_salesforce(sf, df_to_upload, 'Account', 'insert')

        #-----------------------------------------------------------------------
        # 4) query the inserted records and load results into a new DataFrame
        #-----------------------------------------------------------------------
        # query the inserted records and load results into a new DataFrame
        account_query = """SELECT   AccountNumber,
                                    Name,
                                    NumberOfEmployees,
                                    NumberOfLocations__c,
                                    Phone,
                                    SLA__c,
                                    SLASerialNumber__c,
                                    Account_Number_ExternaL_ID__c
                            FROM Account WHERE CreatedBy.Name = 'Timothy Kornish'"""
        # query salesforce and return the accounts just inserted
        account_query_results = self.sf_utils.query_salesforce(sf, account_query)
        # convert query results to a dataframe
        accounts_df = self.sf_utils.load_query_with_lookups_into_dataframe(account_query_results)

        #-----------------------------------------------------------------------
        # 6) cleanup environment, delete inserted records
        #-----------------------------------------------------------------------
        # create query for records to delete
        # add field in salesforce and to df, unit_test_migrated_record = True,
        # only delete these records in unit test
        account_query = "SELECT Id FROM Account WHERE CreatedBy.Name = 'Timothy Kornish'"
        # query salesforce and return the accounts to be deleted
        account_query_results = self.sf_utils.query_salesforce(sf, account_query)
        # convert query results to a dataframe
        accounts_to_delete_df = self.sf_utils.load_query_with_lookups_into_dataframe(account_query_results)
        # delete the test records in salesforce
        self.sf_utils.upload_dataframe_to_salesforce(sf, accounts_to_delete_df, 'Account', 'delete')


        #-----------------------------------------------------------------------
        # 6) pandas testing assert dataframes are equal (original dataframe, queried dataframe)
        #-----------------------------------------------------------------------
        # set the column datatypes so the comparison is on the data and not datatypes
        column_types = ('int', 'str', 'int', 'int', 'str', 'str', 'int', 'str')
        # reformat the column datatypes of queried dataframe before comparing
        formatted_accounts_df = self.utils.format_columns_dtypes(accounts_df, column_types, True)
        # reformat the column datatypes of the inserted dataframe before comparing
        formatted_df_to_upload = self.utils.format_columns_dtypes(df_to_upload, column_types, True)

        # Assert the two dataframes are equal - finishing test
        assert_frame_equal(formatted_accounts_df, formatted_df_to_upload)



    def test_successful_salesforce_login_update_then_query(self):
        """Description: This test performs the following operations

        1) create login to salesforce
        2) load a dictionary record into a dataframe of length 5
        3) insert a dataframe of records into salesforce then upload the updated dataframe to Salesforce to update 5 records
        4) query the updated record and load results into a new DataFrame
        5) clean up environment, delete inserted records
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

        DML operations included: UPDATE, SELECT
        """
        #-----------------------------------------------------------------------
        # 1) create login to salesforce
        #-----------------------------------------------------------------------
        # get username from credentials
        username = self.credentials.get_username("Salesforce", "Dev")
        # get password from credentials
        password = self.credentials.get_password("Salesforce", "Dev")
        # get login token from credentials
        token = self.credentials.get_token("Salesforce", "Dev")
        # create a instance of simple_salesforce to query and perform operations against salesforce with
        sf = self.sf_utils.login_to_salesForce(username, password, token)

        #-----------------------------------------------------------------------
        # 2) load a mock csv data into a dataframe and keep a slice of length 10
        #-----------------------------------------------------------------------
        # set record start index
        starting_index = 0
        # set number of records to keep
        number_of_records = 5
        # select only 10 records
        df_to_upload = self.utils.get_slice_of_dataframe(self.mock_data_df, starting_index, number_of_records)

        #-----------------------------------------------------------------------
        # 3) insert a dataframe of records into salesforce then upload the
        # updated dataframe to Salesforce to update 5 records
        #-----------------------------------------------------------------------
        # upload the records to salesforce
        self.sf_utils.upload_dataframe_to_salesforce(sf, df_to_upload, 'Account', 'insert')

        # update values in the DataFrame
        # add new column called type and set all accounts to Prospect
        df_to_upload .loc[:,"Type"] = "Prospect"
        # add new column called Industry and set all accounts to government
        df_to_upload .loc[:,"Industry"] = "Government"

        # upload the records to salesforce
        self.sf_utils.upload_dataframe_to_salesforce(sf, df_to_upload, 'Account', 'update')

        #-----------------------------------------------------------------------
        # 4) query the updated records and load results into a new DataFrame
        #-----------------------------------------------------------------------
        # query the inserted records and load results into a new DataFrame
        account_query = """SELECT   AccountNumber,
                                    Name,
                                    NumberOfEmployees,
                                    NumberOfLocations__c,
                                    Phone,
                                    SLA__c,
                                    SLASerialNumber__c,
                                    Account_Number_External_ID__c,
                                    Type,
                                    Industry
                            FROM Account WHERE CreatedBy.Name = 'Timothy Kornish'"""
        # query salesforce and return the accounts just inserted
        account_query_results = self.sf_utils.query_salesforce(sf, account_query)
        # convert query results to a dataframe
        accounts_df = self.sf_utils.load_query_with_lookups_into_dataframe(account_query_results)

        #-----------------------------------------------------------------------
        # 5) pandas testing assert dataframes are equal (original dataframe, queried dataframe)
        #-----------------------------------------------------------------------
        # set the column datatypes so the comparison is on the data and not datatypes
        column_types = ('int', 'str', 'int', 'int', 'str', 'str', 'int', 'str', 'str', 'str')
        # reformat the column datatypes of queried dataframe before comparing
        formatted_accounts_df = self.utils.format_columns_dtypes(accounts_df, column_types, True)
        # reformat the column datatypes of the inserted dataframe before comparing
        formatted_df_to_upload = self.utils.format_columns_dtypes(df_to_upload, column_types, True)
        # make sure to only compare the updated records
        both_df, left_df, right_df = self.utils.get_df_diffs(formatted_df_to_upload, formatted_accounts_df, left_on = "Account_Number_External_ID__c", right_on = "Account_Number_External_ID__c", suffixes = None)


        # Assert the two dataframes are equal - finishing test
        assert_frame_equal(formatted_accounts_df, both_df)

    def test_successful_salesforce_login_upsert_then_query(self):
        """Description: This test performs the following operations

        1) create login to salesforce
        2) load a dictionary record into a dataframe of length 10
        3) choose 5 that are already loaded and 5 new.
        4) upload the dataframe to Salesforce to upsert all test records
        5) query the record and load results into a new DataFrame
        6) clean up environment, delete inserted records
        7) pandas testing assert dataframes are equal (original dataframe, queried dataframe)

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


if __name__ == '__main__':
    unittest.main()

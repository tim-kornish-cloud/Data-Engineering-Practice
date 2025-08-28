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
        # set saleforce environment to Dev
        self.sf_environment = "Dev"
        # set Salesforce database to Salesforce
        self.sf_database = "Salesforce"
        # create instance of MSSQL utility class
        self.mssql_utils    = MSSQL_Utilities()
        # create instance of MySQL utility class
        self.mysql_utils    = MySQL_Utilities()
        # create instance of EC2_S3 utility class
        self.s3_utils       = EC2_S3_Utilities()
        # create instance of MongoDB utility class
        self.mongosb_utils  = MongoDB_Utilities()

        # set input path for mock data csv
        self.input_csv_file = dir_path + ".\\MockData\\MOCK_DATA_unit_test.csv"
        # read the mock data csv into a pandas dataframe at the beginning of every test
        self.mock_data_df = pd.read_csv(self.input_csv_file)
        # update success file path
        self.update_success_file = dir_path + "\\Output\\UPDATE\\SUCCESS_Update_" + self.sf_environment + "_" + self.sf_database + ".csv"
        # update fallout file path
        self.update_fallout_file = dir_path + "\\Output\\UPDATE\\FALLOUT_Update_" + self.sf_environment + "_" + self.sf_database + ".csv"
        # upsert success file path
        self.upsert_success_file = dir_path + "\\Output\\UPSERT\\SUCCESS_Upsert_" + self.sf_environment + "_" + self.sf_database + ".csv"
        # upsert fallout file path
        self.upsert_fallout_file = dir_path + "\\Output\\UPSERT\\FALLOUT_Upsert_" + self.sf_environment + "_" + self.sf_database + ".csv"

    @unittest.skip("complete, comment line to retest")
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
        username = self.credentials.get_username(self.sf_database, self.sf_environment)
        # get password from credentials
        password = self.credentials.get_password(self.sf_database, self.sf_environment)
        # get login token from credentials
        token = self.credentials.get_token(self.sf_database, self.sf_environment)
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
                                    Account_Number_ExternaL_ID__c,
                                    Unit_test_migrated_record__c
                            FROM Account WHERE Unit_test_migrated_record__c = true"""
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
        account_query = "SELECT Id FROM Account WHERE Unit_test_migrated_record__c = true"
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
        column_types = ('int', 'str', 'int', 'int', 'str', 'str', 'int', 'str', 'bool')
        # reformat the column datatypes of queried dataframe before comparing
        formatted_accounts_df = self.utils.format_columns_dtypes(accounts_df, column_types, True)
        # reformat the column datatypes of the inserted dataframe before comparing
        formatted_df_to_upload = self.utils.format_columns_dtypes(df_to_upload, column_types, True)

        # Assert the two dataframes are equal - finishing test
        assert_frame_equal(formatted_accounts_df, formatted_df_to_upload)

    @unittest.skip("complete, comment line to retest")
    def test_successful_salesforce_login_insert_and_update_then_query(self):
        """Description: This test performs the following operations

        1) create login to salesforce
        2) load a dictionary record into a dataframe of length 5
        3) insert a dataframe of records into salesforce then query inserted records to retrieve salesforce record ID
        4) upload the updated dataframe with Salesforce IDs to Salesforce and update 5 records
        5) query the updated records and load results into a new DataFrame
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

        DML operations included: UPDATE, SELECT
        """
        #-----------------------------------------------------------------------
        # 1) create login to salesforce
        #-----------------------------------------------------------------------
        # get username from credentials
        username = self.credentials.get_username(self.sf_database, self.sf_environment)
        # get password from credentials
        password = self.credentials.get_password(self.sf_database, self.sf_environment)
        # get login token from credentials
        token = self.credentials.get_token(self.sf_database, self.sf_environment)
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

        # match queried accounts with CSV accounts based on join of accountNumber field
        # query string to select records from salesforce
        # before uploading with a delete  DML operation
        account_query = "SELECT Id, Account_Number_External_ID__c FROM Account WHERE Unit_test_migrated_record__c = true"

        # query salesforce and return the accounts to be deleted
        account_query_results = self.sf_utils.query_salesforce(sf, account_query)
        # convert query results to a dataframe
        accounts_df = self.sf_utils.load_query_with_lookups_into_dataframe(account_query_results)
        # encode the dataframe before uploading to delete
        accounts_df = self.utils.encode_df(accounts_df)
        # merge the csv data with the salesforce data to match SF Ids to the CSV accounts
        accounts_to_update_df = self.utils.merge_dfs(accounts_df, df_to_upload, left_on = ['Account_Number_External_ID__c'], right_on = ['Account_Number_External_ID__c'], how = 'inner', suffixes = ('_SF', '_CSV'), indicator = False)


        #-----------------------------------------------------------------------
        # 4) query inserted records to retrieve salesforce record IDs
        #-----------------------------------------------------------------------
        # add new columns in the DataFrame to update records in salesforce
        # add new column called type and set all accounts to Prospect
        accounts_to_update_df.loc[:,"Type"] = "Prospect"
        # add new column called Industry and set all accounts to government
        accounts_to_update_df.loc[:,"Industry"] = "Government"
        # upload the records to salesforce
        self.sf_utils.upload_dataframe_to_salesforce(sf, accounts_to_update_df, 'Account', 'update', self.update_success_file, self.update_fallout_file)

        #-----------------------------------------------------------------------
        # 5) query the updated records and load results into a new DataFrame
        #-----------------------------------------------------------------------
        # query the inserted records and load results into a new DataFrame
        account_query = """SELECT   Id,
                                    AccountNumber,
                                    Name,
                                    NumberOfEmployees,
                                    NumberOfLocations__c,
                                    Phone,
                                    SLA__c,
                                    SLASerialNumber__c,
                                    Account_Number_External_ID__c,
                                    Unit_test_migrated_record__c,
                                    Type,
                                    Industry
                            FROM Account WHERE Unit_test_migrated_record__c = true"""
        # query salesforce and return the accounts just inserted
        account_query_results = self.sf_utils.query_salesforce(sf, account_query)
        # convert query results to a dataframe
        accounts_df = self.sf_utils.load_query_with_lookups_into_dataframe(account_query_results)

        #-----------------------------------------------------------------------
        # 6) clean up environment, delete inserted records
        #-----------------------------------------------------------------------
        # create query for records to delete
        # add field in salesforce and to df, unit_test_migrated_record = True,
        # only delete these records in unit test
        account_query = "SELECT Id FROM Account WHERE Unit_test_migrated_record__c = true"
        # query salesforce and return the accounts to be deleted
        account_query_results = self.sf_utils.query_salesforce(sf, account_query)
        # convert query results to a dataframe
        accounts_to_delete_df = self.sf_utils.load_query_with_lookups_into_dataframe(account_query_results)
        # delete the test records in salesforce
        self.sf_utils.upload_dataframe_to_salesforce(sf, accounts_to_delete_df, 'Account', 'delete')

        #-----------------------------------------------------------------------
        # 7) pandas testing assert dataframes are equal (original dataframe, queried dataframe)
        #-----------------------------------------------------------------------
        # re-order columns to match for comparison
        accounts_to_update_df = accounts_to_update_df[list(accounts_df.columns)]
        # set the column datatypes so the comparison is on the data and not datatypes
        column_types = ('str', 'int', 'str', 'int', 'int', 'str', 'str', 'int', 'str', 'bool', 'str', 'str')
        # reformat the column datatypes of queried dataframe before comparing
        formatted_accounts_df = self.utils.format_columns_dtypes(accounts_df, column_types, True)
        # reformat the column datatypes of the inserted dataframe before comparing
        formatted_accounts_to_update_df = self.utils.format_columns_dtypes(accounts_to_update_df, column_types, True)
        # make sure to only compare the updated records
        both_df, left_df, right_df = self.utils.get_df_diffs(formatted_accounts_to_update_df, formatted_accounts_df, left_on = ["Account_Number_External_ID__c"], right_on = ["Account_Number_External_ID__c"])
        # drop all extra columns
        both_df = both_df[["Id_left", "AccountNumber_left", "Name_left", "NumberOfEmployees_left",
        "NumberofLocations__c_left", "Phone_left", "SLA__c_left","SLASerialNumber__c_left", "Unit_test_migrated_record__c_left",
        "Account_Number_External_ID__c", "Type_left","Industry_left"]]
        # rename columns to match for assert comparison
        both_df.rename(columns = {"Id_left" : "Id" , "AccountNumber_left" : "AccountNumber", "Name_left" : "Name", "NumberOfEmployees_left" : "NumberOfEmployees",
        "NumberofLocations__c_left" : "NumberofLocations__c", "Phone_left" : "Phone", "SLA__c_left" : "SLA__c",
        "SLASerialNumber__c_left" : "SLASerialNumber__c", "Unit_test_migrated_record__c_left" : "Unit_test_migrated_record__c", "Type_left" : "Type", "Industry_left" : "Industry"}, inplace = True)
        # remove extra columns to match for assert comparison
        both_df = both_df[["Id", "AccountNumber", "Name", "NumberOfEmployees", "NumberofLocations__c", "Phone", "SLA__c","SLASerialNumber__c", "Unit_test_migrated_record__c",
        "Account_Number_External_ID__c", "Type","Industry"]]
        # re-order columns for assert  comparison
        formatted_accounts_to_update_df = formatted_accounts_to_update_df[list(both_df.columns)]
        # Assert the two dataframes are equal - finishing test
        # formatted_accounts_to_update_df and both_df shoould both be the same size with the same values
        assert_frame_equal(formatted_accounts_to_update_df, both_df)

    #@unittest.skip("in progress")
    def test_successful_salesforce_login_insert_and_upsert_then_query(self):
        """Description: This test performs the following operations

        1) create login to salesforce
        2) load a dictionary record into a dataframe of length 10
        3) insert a dataframe of records into salesforce
        4) get 5 new records and 5 overlapping records then upload the dataframe to Salesforce to upsert all test records
        5) query the upserted records and load results into a new DataFrame
        6) query the insert and not upserted records to confirm they didn't get updated
        7) clean up environment, delete inserted records
        8) pandas testing assert insert only dataframes are equal (insert dataframe, queried insert records dataframe)
        9) pandas testing assert upsert dataframes are equal (upsert dataframe, queried upsert records dataframe)

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
        #-----------------------------------------------------------------------
        # 1) create login to salesforce
        #-----------------------------------------------------------------------
        # get username from credentials
        username = self.credentials.get_username(self.sf_database, self.sf_environment)
        # get password from credentials
        password = self.credentials.get_password(self.sf_database, self.sf_environment)
        # get login token from credentials
        token = self.credentials.get_token(self.sf_database, self.sf_environment)
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
        accounts_to_insert_df = self.utils.get_slice_of_dataframe(self.mock_data_df, starting_index, number_of_records)
        # add new columns in the DataFrame to help identify records in this unit test
        # add new column called type and set all accounts to Technology Partner
        accounts_to_insert_df["Type"] = "Technology Partner"
        # add new column called Industry and set all accounts to Engineering
        accounts_to_insert_df["Industry"] = "Engineering"
        # add custom field for tracking records insert records, set to true for first 5 records
        accounts_to_insert_df.loc[0:5,"upsert_test_insert_only__c"] = True
        # add custom field for tracking records insert records, set to false for last 5 records
        accounts_to_insert_df.loc[5:10,"upsert_test_insert_only__c"] = False
        # add field to track if the record is upserted through an upsert call
        accounts_to_insert_df.loc[0:10,"upsert_test_upserted__c"] = False

        #-----------------------------------------------------------------------
        # 3) insert a dataframe of records into salesforce
        #-----------------------------------------------------------------------
        # upload the records to salesforce
        self.sf_utils.upload_dataframe_to_salesforce(sf, accounts_to_insert_df, 'Account', 'insert')

        #-----------------------------------------------------------------------
        # 4) get 5 new records and 5 overlapping records then upload the dataframe to Salesforce to upsert all test records
        #-----------------------------------------------------------------------
        # set record start index
        starting_index = 5
        # set number of records to keep
        number_of_records = 10
        # select only 10 records
        accounts_to_upsert_df = self.utils.get_slice_of_dataframe(self.mock_data_df, starting_index, number_of_records)

        # add new columns in the DataFrame to update records in salesforce
        # add new column called type and set all accounts to Prospect
        accounts_to_upsert_df.loc[:,"Type"] = "Prospect"
        # add new column called Industry and set all accounts to government
        accounts_to_upsert_df.loc[:,"Industry"] = "Government"
        # make sure to set the testing field to false for upserted records, overwrite 5 of the intersted records value
        accounts_to_upsert_df.loc[:,"upsert_test_insert_only__c"] = False
        # add field to track if the record is upserted through an upsert call
        accounts_to_upsert_df.loc[:,"upsert_test_upserted__c"] = True
        # upload the records to salesforce
        self.sf_utils.upload_dataframe_to_salesforce(sf, accounts_to_upsert_df, 'Account', 'upsert', success_file = self.upsert_success_file, fallout_file = self.upsert_fallout_file, external_id_field = "Account_Number_External_ID__c")

        #-----------------------------------------------------------------------
        # 5) query the updated records and load results into a new DataFrame
        #-----------------------------------------------------------------------
        # query the inserted records and load results into a new DataFrame
        queried_upserted_accounts_query = """SELECT AccountNumber,
                                             Name,
                                             NumberOfEmployees,
                                             NumberOfLocations__c,
                                             Phone,
                                             SLA__c,
                                             SLASerialNumber__c,
                                             Account_Number_External_ID__c,
                                             Unit_test_migrated_record__c,
                                             Type,
                                             Industry,
                                             upsert_test_insert_only__c,
                                             upsert_test_upserted__c
                                             FROM Account
                                             WHERE Unit_test_migrated_record__c = true
                                             AND upsert_test_upserted__c = true
                                             AND upsert_test_insert_only__c = false"""
        # query salesforce and return the accounts just inserted
        queried_upserted_accounts_query_results = self.sf_utils.query_salesforce(sf, queried_upserted_accounts_query)
        # convert query results to a dataframe
        queried_upserted_accounts_df = self.sf_utils.load_query_with_lookups_into_dataframe(queried_upserted_accounts_query_results)

        #-----------------------------------------------------------------------
        # 6) query the inserted and non-upserted records to confirm they didn't get updated
        #-----------------------------------------------------------------------
        # query the inserted records and load results into a new DataFrame
        queried_insert_only_accounts_query = """SELECT AccountNumber,
                                                Name,
                                                NumberOfEmployees,
                                                NumberOfLocations__c,
                                                Phone,
                                                SLA__c,
                                                SLASerialNumber__c,
                                                Account_Number_External_ID__c,
                                                Unit_test_migrated_record__c,
                                                Type,
                                                Industry,
                                                upsert_test_insert_only__c,
                                                upsert_test_upserted__c
                                                FROM Account
                                                WHERE Unit_test_migrated_record__c = true
                                                AND upsert_test_upserted__c = false
                                                AND upsert_test_insert_only__c = true"""
        # query salesforce and return the accounts just inserted
        queried_insert_only_accounts_query_results = self.sf_utils.query_salesforce(sf, queried_insert_only_accounts_query)
        # convert query results to a dataframe
        queried_insert_only_accounts_df = self.sf_utils.load_query_with_lookups_into_dataframe(queried_insert_only_accounts_query_results)

        #-----------------------------------------------------------------------
        # 7) clean up environment, delete inserted records
        #-----------------------------------------------------------------------
        # create query for records to delete
        # add field in salesforce and to df, unit_test_migrated_record = True,
        # only delete these records in unit test
        account_query = "SELECT Id FROM Account WHERE Unit_test_migrated_record__c = true"
        # query salesforce and return the accounts to be deleted
        account_query_results = self.sf_utils.query_salesforce(sf, account_query)
        # convert query results to a dataframe
        accounts_to_delete_df = self.sf_utils.load_query_with_lookups_into_dataframe(account_query_results)
        # delete the test records in salesforce
        self.sf_utils.upload_dataframe_to_salesforce(sf, accounts_to_delete_df, 'Account', 'delete')

        #-----------------------------------------------------------------------
        # 8) pandas testing assert dataframes are equal (original dataframe, queried dataframe)
        # 8.1) assert insert through insert call only record's dataframes are equal and unchanged by upsert call
        # 8.2) assert inserted then updated through upsert record's dataframes are equal
        # 8.3) assert inserted through upsert call record's dataframes are equal
        #-----------------------------------------------------------------------
        # set the column datatypes so the comparison is on the data and not datatypes of all three comparisons listed above
        column_types = ('int', 'str', 'int', 'int', 'str', 'str', 'int', 'str', 'bool', 'str', 'str', 'bool', 'bool')

        print("1")
        print(accounts_to_insert_df.columns)
        print(accounts_to_insert_df.head(10))
        print("2")
        print(accounts_to_upsert_df.columns)
        print(accounts_to_upsert_df.head(10))
        print("3")
        print(queried_insert_only_accounts_df.columns)
        print(queried_insert_only_accounts_df.head(10))
        print("4")
        print(queried_upserted_accounts_df.columns)
        print(queried_upserted_accounts_df.head(10))


        # 8.1)
        print("8.1)")
        # separate the inserted records based on if they will receive an update from the upsert call
        # do not need to reset the index of any dataframe in 8.1 since the slicing starts at index 0
        # compare insert only, confirm not altered by upsert call
        insert_only_df = accounts_to_insert_df.iloc[0:5]
        print(insert_only_df.columns)
        # queried_insert_only_accounts_df
        # queried_insert_only_accounts_df
        print(queried_insert_only_accounts_df.columns)
        #reformat columns of insert only accounts
        formatted_insert_only_df = self.utils.format_columns_dtypes(insert_only_df, column_types, True)
        # reset the index of the reformatted dataframe since the index starts at 5 instead of 0
        formatted_insert_only_df.reset_index(inplace = True, drop = True)
        # reformat columns of queried records
        formatted_queried_insert_only_accounts_df = self.utils.format_columns_dtypes(queried_insert_only_accounts_df, column_types, True)
        # reset the index of the reformatted dataframe since the index starts at 5 instead of 0
        formatted_queried_insert_only_accounts_df.reset_index(inplace = True, drop = True)

        # 8.2)
        print("8.2)")
        # compare insert then updated through upsert
        insert_then_upsert_df = accounts_to_upsert_df.iloc[0:5]
        print(insert_then_upsert_df.columns)
        #
        queried_updated_through_upsert_accounts_df = queried_upserted_accounts_df.iloc[0:5]
        print(queried_updated_through_upsert_accounts_df.columns)
        #reformat columns of insert only accounts
        formatted_insert_then_upsert_df = self.utils.format_columns_dtypes(insert_then_upsert_df, column_types, True)
        # reset the index of the reformatted dataframe since the index starts at 5 instead of 0
        formatted_insert_then_upsert_df.reset_index(inplace = True, drop = True)
        # reformat columns of queried records
        formatted_queried_updated_through_upsert_accounts_df = self.utils.format_columns_dtypes(queried_updated_through_upsert_accounts_df, column_types, True)
        # reset the index of the reformatted dataframe since the index starts at 5 instead of 0
        formatted_queried_updated_through_upsert_accounts_df.reset_index(inplace = True, drop = True)

        # 8.3)
        print("8.3)")
        # compare inserted through upsert call
        insert_only_from_upsert_df = accounts_to_upsert_df.iloc[5:10]
        print(insert_only_from_upsert_df.columns)
        queried_inserted_through_upsert_accounts_df = queried_upserted_accounts_df.iloc[5:10]
        print(queried_inserted_through_upsert_accounts_df.columns)
        #reformat columns of insert only accounts
        formatted_insert_only_from_upsert_df = self.utils.format_columns_dtypes(insert_only_from_upsert_df, column_types, True)
        # reset the index of the reformatted dataframe since the index starts at 5 instead of 0
        formatted_insert_only_from_upsert_df.reset_index(inplace = True, drop = True)
        # reformat columns of queried records
        formatted_queried_inserted_through_upsert_accounts_df = self.utils.format_columns_dtypes(queried_inserted_through_upsert_accounts_df, column_types, True)
        # reset the index of the reformatted dataframe since the index starts at 5 instead of 0
        formatted_queried_inserted_through_upsert_accounts_df.reset_index(inplace = True, drop = True)

        # assert 8.1)
        assert_frame_equal(formatted_insert_only_df, formatted_queried_insert_only_accounts_df)
        # assert 8.2)
        assert_frame_equal(formatted_insert_then_upsert_df, formatted_queried_updated_through_upsert_accounts_df)
        # assert 8.3)
        assert_frame_equal(formatted_insert_only_from_upsert_df, formatted_queried_inserted_through_upsert_accounts_df)


if __name__ == '__main__':
    unittest.main()

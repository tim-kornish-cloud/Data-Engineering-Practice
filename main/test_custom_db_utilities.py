import unittest
from unittest import mock
from unittest.mock import patch
from pandas.testing import assert_frame_equal, assert_series_equal
import pandas as pd
import numpy as np
from credentials import Credentials
from custom_db_utilities import  Salesforce_Utilities, EC2_S3_Utilities

# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

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
        self.credentials = {
            "Salesforce" : {
                "username" : Cred.get_username(self.sf_database, self.sf_environment),
                "password" : Cred.get_password(self.sf_database, self.sf_environment),
                "token" : Cred.get_token(self.sf_database, self.sf_environment)
            },
            "MSSQL" : {
                "token" : Cred.get_token(self.sf_database, self.sf_environment)
            }
        }

    @mock.patch('Salesforce_Utilities.login_to_salesForce') # Patch the function that establishes connection
    def test_successful_salesforce_login_insert_then_query(self, mock_connect_to_db):


    @mock.patch('Salesforce_Utilities.login_to_salesForce')
    def test_failed_salesforce_login_insert_then_query(self, mock_connect_to_db):
        # Configure the mock to raise a ConnectionError
        mock_connect_to_db.side_effect = ConnectionError("Connection refused")

        # Assert that calling the function raises the expected exception
        config = {'host': 'invalid', 'database': 'nonexistent'}
        with self.assertRaises(ConnectionError):
            login_to_salesForce(config)

        # Assert that the connect_to_db function was called
        mock_connect_to_db.assert_called_once_with(config)

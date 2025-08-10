from ctypes import util
import numpy as np
import pandas as pd
from simple_salesforce import Salesforce
import pyodbc
from datetime import datetime
from collections import OrderedDict
import time

import logging as log
import coloredlogs

coloredlogs.install()
log.basicConfig(level = log.DEBUG)

class Custom_SF_Utilities:
    def __init__(self):
        """Constructor Parameters:
            currently not neccessary
        """
    def login_to_salesForce(self, username, password, security_token, environment = ''):
        """
        Parameters:

        Username        - string, salesforce Username
        Password        - string, salesforce Password
        security_token  - string, salesforce token
        environments    - string, used for logger to state which org being logged into
        Return:

        sf              - Salesforce instance to query against
        """
        # log status to console
        log.info('[Logging into source org: ' + environment + ']')
        # log into salesforce using simple_salesforce
        sf = Salesforce(username = username,
                        password = password,
                        security_token = security_token
                        )
        # log to console the login was successful. if not successful,
        # there will be a error from Salesforce on the console
        log.info('[Logged in successfully]')
        # return instance of salesforce to perform operations with
        return sf

    

    def reformat_df_to_SF_records(self, df):
        """
        Reformat df into list of dicts where each dict is a SF record
        Parameters:

        df - DataFrame, Salesforce records

        Return:

        sf_records - list of dicts, each dict is a single layer deep, no nesting.
        """
        #log to console, reformatting records to json format
        log.info('[Reformatting data for SF JSON]')
        #conver the dataframe to a json dictionary
        sf_records = df.to_dict('records')
        #return the records in salesforce format
        return sf_records

    def upload_records_to_salesforce(self, sf, df, object_name, dml_operation, success_file = None, fallout_file = None, batch_size = 1000, external_id_field=None, time_delay = None):
        """
        Parameters:

        sf                  - simple_salesforce instance used to log in and perform operations again Salesforce
        df                  - pandas data frame of the data to be uploaded, formatting will occur in function
        object_name         - Salesforce object to perform operations against, both standard and custom objects
        dml_operation       - insert/upsert/update/delete, hard_delete should conceptually work, but I haven't tested it
        success_file        - string, path to store the success output file
        fallout_file        - string, path to store the fallout output file
        batch_size          - set batch size of records to upload in a single attempt
        external_id_field   - string, name of the external id field
        time_delay          - add a time delay between batch record uploads in case custom code needs to process between batches.

        Return:

        array of length 2, the fallout and success results separated in two DataFrames
        """
        # keep track of records attempted
        records_loaded = 0
        # keep  track of how many records successfully loaded
        passing = 0
        # keep track of how many records unsuccessfully loaded
        fallout = 0
        # array of the results to return at end of function
        results_list = []
        # quick check that we're not attempting to load 0 records, quit out immediately if so
        if len(df) != 0:
            # reformat the records from a pandas dataframe to JSON in salesforce compatibale format
            records_to_commit = self.reformat_df_to_SF_records(df)
            # record how many records are going to be attempted
            records_count = len(records_to_commit)
            # log to console status
            log.info('[Starting DML.. records to ' + dml_operation + ': ' + str(records_count) + ']')
            # perform as many loops as necessary to upload the records on the selected batch size.
            for index in range(0, records_count, batch_size):
                # if there are more records to upload after the current batch upload a full batch,
                if index+batch_size <= records_count:
                    # select a full batch of data
                    data = records_to_commit[index:index+batch_size]
                # the number of records remaining to upload is less than the selected batch size
                else:
                    # adjust how many records to select in the last batch to upload
                    data = records_to_commit[index:records_count]
                # perform insert/upsert/update/delete operations using the submit_dml function
                results = sf.bulk.submit_dml(object_name, dml_operation, data, external_id_field)
                # convert the list of json records that was attempted into a pandas dataframe
                data_df = pd.DataFrame(data)
                # convert the results from the upload into a pandas dataframe
                # add a suffix to all new columns created from the upload
                results_df = pd.DataFrame(results).add_prefix('RESULTS_')
                # split the results into two group, passing and fallout
                # passing is success = true
                passing = passing + len(results_df[results_df['RESULTS_success'] == True])
                # fallout is success = false
                fallout = fallout + len(results_df[results_df['RESULTS_success'] == False])
                # concat the results of this batch to the dataframe the holds the results of the entire df being batch
                results_df = pd.concat([data_df, results_df.reindex(data_df.index)], axis = 1)
                # upload the resulting dataframe into an array
                results_list.append(results_df)
                # log the status of how many records passed vs failed
                log.info('[' + str(passing) + '/' + str(records_count) + ' rows of data - ' + dml_operation + ' rows of data loaded, failed rows:' + str(fallout) + '...]')
                # if using a time delay between uploads, extecute the delay here at the end of the loop
                if time_delay != None:
                    # time delay
                    time.sleep(time_delay)
            #full list of every record attempted
            results_df = pd.concat(results_list)
            # split the results int passing and fallout again
            # passing : 'RESULTS_success' == True
            passing_df = results_df[results_df['RESULTS_success'] == True]
            # fallout : 'RESULTS_success' == False
            fallout_df = results_df[results_df['RESULTS_success'] == False]

            #if a success file pathway is added, write the success datafame to the csv
            if success_file != None:
                #open the file location in write mode
                with open(success_file, mode = 'w', newline = '\n') as file:
                    #write the dataframe to the file using a commma as the delimeter
                    passing_df.to_csv(file, sep = ',', index = False)
            #if a fallout file pathway is added, write the fallout datafame to the csv
            if fallout_file != None:
                #open the file location in write mode
                with open(fallout_file, mode = 'w', newline = '\n') as file:
                    #write the dataframe to the file using a commma as the delimeter
                    fallout_df.to_csv(file, sep = ',', index = False)
            # return both the passing and fallout dataframes
            return [passing_df, fallout_df]
        #no records are in the dataframe, nothing to process
        else:
            #log to console, nothing included in dataframe to process
            log.info('[No Records to process]')

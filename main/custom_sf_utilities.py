"""
Author: Timothy Kornish
CreatedDate: August - 10 - 2025
Description: A backend utility file for logging into Salesforce,
             querying results with added pre-processing, post-processing,
             and logging to the console for time recording
             of execution start and end results.

"""

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
           - currently no customization used.
        """
    def login_to_salesForce(self, username, password, security_token, environment = ''):
        """
        Description: log into a Salesforce or and return salesforce client
                     to operate with.
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

    def query_salesforce(self, sf, query, include_deleted = False):
        """
        Description: upload a SOQL query to salesforce and return
                     a JSON object full of records.
        Parameters:

        sf              - Salesforce instance to query against
        query           - string, SOQL query

        Return:

        query_results   - JSON formatted records
        """
        # log status to console of querying Salesforce
        log.info('[Querying Salesforce orgs, include deleted records: ' + str(include_deleted) + ']')
        # query salesforce and return results in json format
        query_results = sf.query_all(query, include_deleted = include_deleted)
        # return the json query results
        return query_results

    def load_query_into_DataFrame(self, query_results):
        """
        Description: intermediary function to load SOQL query
                     that has lookup fields, requires more processing time.
        Parameters:

        query_results - OrderedDict, JSON formatted records

        Return:

        df            - DataFrame of the Salesforce Records
        """
        # use function to process query since it
        # has log to detect if query uses lookups or not
        return Utilities.load_query_with_lookups_into_DataFrame(self, query_results)

    def un_nest_lookups(self, df, continue_loop = False, use_subset = True, subset_size = 1000):
        """
        Description: Process dataframe created from results of Salesforce Query
                     loop through all the columns of the dataframe and
                     check if each column has a nested dictionary indicating
                     this column is a nested lookup to un nest.
        Parameters:

        df            - DataFrame of the Salesforce Records
        continue_loop - recursive parameter to continue unpacking nested columns when set to true
        use_subset    - use batches
        subset_size   - batch size default to 1000

        Return:

        df            - DataFrame of the Salesforce Records
        """
        # use function to process query since it
        # loop through each column in the dataframe
        for column in df.columns:
            # log to console which column is being checked
            log.info('[Checking if column: ' + column + ' is a lookup]')
            # use batches instead of the entire dataset
            if use_subset:
                # only check a batch size of data for lookups (only batch number of rows)
                list_to_check = [True if type(row[column]) == OrderedDict else False for index, row in df.head(subset_size).iterrows()]
            # dont use batches, use the entire dataset
            else:
                # check the entire dataset for lookups (load every row, more data intensive)
                list_to_check = [True if type(row[column]) == OrderedDict else False for index, row in df.iterrows()]
            # check if the list_to_check has any values created needing unnesting
            column_contains_nested_values = np.any(list_to_check)
            # if there are nested columns, pull the json object out
            # and re-insert it into a flat structure
            if column_contains_nested_values == True:
                # create a temporary new dataframe with column count = 1
                new_df = df[column].apply(pd.Series)
                # remove attributes columns if exists to avoid parsing issue.
                if 'attributes' in new_df.columns:
                    # drop attributes columns,
                    # creates duplicate with original layer if left in
                    new_df.drop('attributes', axis = 1, inplace = True)
                # modify name of unnested column
                new_df = new_df.add_prefix(column + ".")
                # add unnested column to the dataframe
                df = pd.concat([df.drop(column, axis = 1), new_df], axis = 1)

        # parse through all columns checking for any deeper unnested columns
        # columns can have multiple layers of nesting,
        # this function works one layer of all columns at a time
        # not by one column finding every layer at a time
        for column in df.columns:
            # check if any column has type of ordered dict needing unnesting
            if type(df[column][0]) == OrderedDict:
                #continue loop
                continue_loop = True
        # Recursion check, if more unnested columns exist, go again
        if continue_loop:
            # recursive loop back again.
            # note the continue loop parameter is set to false by defulat when not included
            return self.un_nest_lookups(df)
        # no more nested columns found to unnest, function is complete, return dataframe
        else:
            # return results of query with every columns properly separated.
            return df

    def load_query_with_lookups_into_DataFrame(self, query_results, use_subset = True, subset_size = 1000):
        """
        Description: Load SOQL query that has lookup fields, requires more processing time.
        Parameters:

        query_results - OrderedDict, JSON formatted records

        Return:

        df            - DataFrame of the Salesforce Records
        """

        # log info to console
        log.info('[loading query results into DataFrames]')
        # load query results json into a diction, then convert the dictionary
        # to a pandas Dataframe
        df = pd.DataFrame.from_dict(dict(query_results)['records'])
        # check if there are nested object fields in the query results
        if 'attributes' in df.columns:
            # drop this column to avoid issue with unnesting the lookup fields
             df.drop(['attributes'], axis = 1, inplace = True)
        # log to console
        log.info('[Unnesting columns for DF with: ' + str(len(df)) + ' records]')
        # unnest lookup fields from query onto a flat array and return as a dataframe
        df = self.un_nest_lookups(df, use_subset = use_subset, subset_size = subset_size)
        # where a notnull NaN value is found, replace with None
        df = df.where((pd.notnull(df)), None)
        # log status of unnesting lookups into a new dataframe
        log.info('[loaded ' + str(len(df)) + ' records into DataFrame]')
        # return the query results as a pandas dataframe
        return df

    def reformat_df_to_SF_records(self, df):
        """
        Description: Reformat df into list of dicts where each dict is a SF record
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
        Description: upload dataframe of records to salesforce with dml operation.
                     This function includes pre processing of dataframe to json for
                     as well as post processing of results into separate
                     success and fallout dataframes writing output to select files
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

class Custom_MSSQL_Utilities:
    def __init__(self):
        """Constructor Parameters:
           - currently no customization used.
        """

    def login_to_MSSQL(self, server, database, username, password, driver = 'SQL Server'):
        """
        Description: login to a MSSQL server and return a cursor object to query with
        Parameters:

        driver          - SQL Server Driver
        server          - IP address of server, I.E. 127.0.0.1
        database        - Database name
        Username        - string, salesforce Username
        Password        - string, salesforce Password

        Return:

        cursor          - cursor to execute queries with
        """
        # log to console status of logging into database
        log.info('[Logging into MS SQL DB: ' + database + ']')
        # create instance of cursor to connect to MSSQL database.
        cursor_connection = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}', host=server, database=database,
                       user=username, password=password)
        # log to console the cursor is created
        log.info('[Creating Cursor]')
        #convert the instance to a cursor
        cursor = cursor_connection.cursor()
        #return the cursor to use to query against the database
        return cursor

    def query_MSSQL_return_DataFrame(self, query, cursor):
        """
        Description: query a MSSQL server with a logged in cursor and
        process results into a pandas dataframe the return the dataframe.
        Parameters:

        query           - query string
        cursor          - cursor creating upon login to execute the query
        Return:

        DataFrame       - Pandas DataFrame
        """
        # log to console beginning query against mssql database
        log.info('[Querying MS SQL DB...]')
        # execute query with cursor
        cursor = cursor.execute(query)
        # convert the results into a list of columns
        columns = [column[0] for column in cursor.description]
        # log to console status of querying records
        log.info('[Condensing results into Dict...]')
        # convert the columns and rows of data into a list of dicts
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        # log to console status of querying records
        log.info('[transforming Dict into DataFrame...]')
        # convert the list of dicts into a pandas dataframe
        results_df = pd.DataFrame(results)
        # log to console status of querying records
        log.info('[loaded ' + str(len(results_df)) + ' records into DataFrame]')
        # return the results of the query as a pandas data frame
        return results_df

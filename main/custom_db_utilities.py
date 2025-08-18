"""
Author: Timothy Kornish
CreatedDate: August - 10 - 2025
Description: A backend utility file for logging into Salesforce,
             querying results with added pre-processing, post-processing,
             and logging to the console for time recording
             of execution start and end results.

"""
#util libraries
from ctypes import util
from datetime import datetime
from collections import OrderedDict
import time
import logging as log
import coloredlogs
#pandas and numpy
import numpy as np
import pandas as pd
#salesforce connector
from simple_salesforce import Salesforce
#MSSQL connector
import pyodbc
#AWS S3 EC2 connector
import boto3
import awswrangler as wr
#mysql connector
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pymysql
import mysql.connector

# initialize the console logging to aid in time estimate of execution scripts
coloredlogs.install()
# set debug level for which debugging is output to console,
# currently only using info debug level comments
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

        Return: sf      - Salesforce instance to query against
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

        Return: query_results - JSON formatted records
        """
        # log status to console of querying Salesforce
        log.info('[Querying Salesforce orgs, include deleted records: ' + str(include_deleted) + ']')
        # query salesforce and return results in json format
        query_results = sf.query_all(query, include_deleted = include_deleted)
        # return the json query results
        return query_results

    def format_date_to_salesforce_date(self, df, columns, format = '%m/%d/%Y'):
        """
        Description: format specified columns in a dataframe to be compatible with Salesforce
        Parameters:

        df          - pandas.DataFrame
        columns     - list of string or string column names to format
        format      - default salesforce format = '%m/%d/%Y'

        Return: query_results - JSON formatted records
        """
        #setup df that will be returned by function
        return_df = df
        # check if formatting a single column
        if type(columns) == str:
            # reformat the single column
            return_df[columns] = pd.to_datetime(df[columns], format = format).dt.normalize().apply(lambda x : str(x)[:10])
        # formatting list of columns
        elif type(columns) == list:
            # loop through list of columns to format one by one
            for column in columns:
                # format the column in this loop of the list
                return_df[column] = pd.to_datetime(df[column], format = format).dt.normalize().apply(lambda x : str(x)[:10])
                # replace blanks due to reformat
                return_df.replace({column : {'NaT' : None, '' : None}}, inplace = True)
        # return the reformatted dataframe
        return return_df

    def load_query_into_DataFrame(self, query_results):
        """
        Description: intermediary function to load SOQL query
                     that has lookup fields, requires more processing time.
                     By default auto sends to the flatten_lookup_fieldname_hierarchy function, can add parameter to assume no lookups in query
        Parameters:

        query_results - OrderedDict, JSON formatted records

        Return: pandas.DataFrame - DataFrame of the Salesforce Records
        """
        # use function to process query since it
        # has log to detect if query uses lookups or not
        return Utilities.load_query_with_lookups_into_DataFrame(self, query_results)

    def flatten_lookup_fieldname_hierarchy(self, df, continue_loop = False, use_subset = True, subset_size = 1000):
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

        Return: pandas.DataFrame - DataFrame of the Salesforce Records
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
            # note the continue loop parameter is reset to false by defulat when not included
            return self.flatten_lookup_fieldname_hierarchy(df)
        # no more nested columns found to unnest, function is complete, return dataframe
        else:
            # return results of query with every columns properly separated.
            return df

    def load_query_with_lookups_into_DataFrame(self, query_results, use_subset = True, subset_size = 1000):
        """
        Description: Load SOQL query that has lookup fields, requires more processing time.
        Parameters:

        query_results - OrderedDict, JSON formatted records

        Return: pandas.DataFrame - DataFrame of the Salesforce Records
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
        df = self.flatten_lookup_fieldname_hierarchy(df, use_subset = use_subset, subset_size = subset_size)
        # where a notnull NaN value is found, replace with None
        df = df.where((pd.notnull(df)), None)
        # log status of unnesting lookups into a new dataframe
        log.info('[loaded ' + str(len(df)) + ' records into DataFrame]')
        # return the query results as a pandas dataframe
        return df

    def reformat_dataframe_to_salesforce_records(self, df):
        """
        Description: Reformat df into list of dicts where each dict is a SF record
        Parameters:

        df - DataFrame, Salesforce records

        Return: sf_records - list of dicts, each dict is a single layer deep, no nesting.
        """
        #log to console, reformatting records to json format
        log.info('[Reformatting data for SF JSON]')
        #conver the dataframe to a json dictionary
        sf_records = df.to_dict('records')
        #return the records in salesforce format
        return sf_records

    def upload_dataframe_to_salesforce(self, sf, df, object_name, dml_operation, success_file = None, fallout_file = None, batch_size = 1000, external_id_field=None, time_delay = None):
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

        Return: array of length 2, the fallout and success results separated in two DataFrames
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
            records_to_commit = self.reformat_dataframe_to_salesforce_records(df)
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

    def login_to_MSSQL(self,  server = None, database = '', username = None, password = None, use_windows_authentication = True, driver = '{ODBC Driver 17 for SQL Server}',  trusted_connection = 'yes'):
        """
        Description: login to a MSSQL server and return a cursor object to query with
        Parameters:

        driver          - SQL Server Driver use {SQL Driver} or {ODBC Driver 17 for SQL Server}
        server          - IP address of server, I.E. 127.0.0.1
        database        - Database name
        Username        - string, salesforce Username
        Password        - string, salesforce Password

        Return: cursor
        """
        #login using connection string
        if use_windows_authentication:
            # log to console status of logging into database
            log.info('[Logging into MSSQL DB using windows Auth on DB: ' + database + ']')
            #establish a connection
            cursor_connection = pyodbc.connect(driver=driver,
                                               host=server,
                                               database=database,
                                               trusted_connection=trusted_connection)
        #log in using credentials: username/password
        else:
            # log to console status of logging into database
            log.info('[Logging into MSSQL DB using credentials on DB: ' + database + ']')
            # create instance of cursor to connect to MSSQL database.
            cursor_connection = pyodbc.connect(driver=driver, host=server, database=database,
                        user=username, password=password)
        #convert the instance to a cursor
        cursor = cursor_connection.cursor()
        # log to console the cursor is created
        log.info('[Creating Cursor]')
        #return the connection and cursor to use to query against the database
        return (cursor_connection, cursor)

    def query_MSSQL_return_DataFrame(self, query, cursor):
        """
        Description: query a MSSQL server with a logged in cursor and
        process results into a pandas dataframe the return the dataframe.
        Parameters:

        query           - query string
        cursor          - cursor creating upon login to execute the query

        Return: pandas.DataFrame
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

    def insert_dataframe_into_MSSQL_table(self, connection, cursor, df, tablename, column_types = [], cols = '', use_all_columns_in_df = True, close_connection = True):
        """Description: insert a dataframe into a mssql table, the whole dataframe will be inserted
        Parameters:

        connection              - MSSQL database connection
        cursor                  - MSSQL connection cursor
        df                      - dataframe to insert
        tablename               - table name of database to insert records into
        column_types            - set column datatypes before insert, auto datatype setting can sometimes be inaccurate
        cols                    - list of columns, currently experimental
        use_all_columns_in_df   - boolean to use all columns or not, currently experimental
        close_connection        - boolean, close connection after insert.

        return: none - insert records into mssql # DEBUG:
        Current issue 8/11/25:
        # current method causes warning with chained indexing instead of using .loc or .iloc
        # error is just making a getattr call twice instead of once and some other things that improve speed but not a breaking issue
        # will update to .loc/.iloc at a later point, just need a working function for now

        """
        # if the df column list matches the table, use all columns
        if use_all_columns_in_df:
            # generate a list of all columns
            cols = ','.join([k for k in df.dtypes.index])
        # generate a list of '?' to be replaced by the actual values of the dataframe
        params = ','.join('?' * len(df.columns))
        # generate the sql commit with the dataframe
        sql = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(tablename, cols, params)

        #for loop only works when provided a list of column converted types
        log.info('[Converting data types in DataFrame...]')
        #loop through each column to convert the type every value
        for index, col in enumerate(df.columns):
            #confirm the index is still within range of acceptable indexes
            if index < len(column_types):
                #check if type == int
                if column_types[index] == 'int':
                    df[col] = df[col].astype(int)
                #check if type == string
                if column_types[index] == 'str':
                    df[col] = df[col].astype(str)
                #check if type == float
                if column_types[index] == 'float':
                    df[col] = df[col].astype(float)
                #check if type == boolean
                if column_types[index] == 'bool':
                    df[col] = df[col].astype(bool)
        #convert the rows in the dataframe into tuples
        data = [tuple(x) for x in df.values]
        #set the bulk insert for pyodbc cursor.fast_executemany = True
        cursor.fast_executemany = True
        #execute insert of records
        cursor.executemany(sql, data)
        # commit the sql statement
        connection.commit()
        # close the connection if desired
        if close_connection:
            # close the connection
            connection.close()

    def update_rows_in_MSSQL_table(self, connection, cursor,  table_name, columns_to_update, column_values_to_update, where_column_name, where_column_list):
        """
        Description: update multiples columns in MSSQL table from a dataframe on a where in list condition

        sql_update =  example:
        UPDATE <table_name>
        SET <column1_name> = <value, corresponding column1 value>, <column2_name> = <value, corresponding column2 value>,
        WHERE <Where_column_name> in < list of corresponding conditional value>;
        Parameters:

        connection               - MSSQL login connection
        cursor                   - MSSQL connection cursor
        table_name               - table in MSSQL to update
        columns_to_update        - column names in MSSQL table to update
        column_values_to_update  - column values to upload into MSSQL table
        where_column_name        - single field, condition for update
        where_column_list        - list of accepted values for where condition

        Return: None - delete records
        """
        # log to console, creating update statement to upload
        log.info('[Creating Update SQL statement...]')
        # create beginning of update, add table name
        sql_update = "UPDATE " + table_name + " SET "
        # add column names and column values to set in the update
        for index, col in enumerate(columns_to_update):
            #make sure to not grab a value outside of range and not the last row
            if index < len(columns_to_update) - 1:
                # for all lines except the last row, add column name, value, and comma
                sql_update = sql_update + col + " = '" + column_values_to_update[index] + "', "
            # adding string to sql_update for last row to update
            if index == len(columns_to_update) - 1:
                # for last row, add column name and value without a comma
                sql_update = sql_update + col + " = '" + column_values_to_update[index] + "' "
        # add where clause to end of sql string
        sql_update = sql_update + " WHERE " + where_column_name + " IN " + where_column_list + ";"
        #execute the deletion of records
        cursor.execute(sql_update)
        #log to console commiting update to table now
        log.info('[Commiting update to MSSQL table...]')
        # commit the sql statement
        connection.commit()

    def delete_rows_in_MSSQL_table(self, connection, cursor, table_name, column_name, record_list):
        """Description: generate a query string to delete records from a MSSQL table
           Parameters:

           connection               - MSSQL login connection
           cursor                   - MSSQL connection cursor
           table_name               - table in MSSQL to update
           columns_name             - column name in MSSQL table with key used to delete
           record_list              - list of key IDs to delete records

           Return: None - delete records
        """
        # Example with parameterization
        sql_delete = "DELETE FROM " + table_name + " WHERE " + column_name + " IN " + record_list + ";"

        #execute the deletion of records
        cursor.execute(sql_delete)
        # commit the sql statement
        connection.commit()

class Custom_MySQL_Utilities:
    def __init__(self):
        """Constructor Parameters:
           - currently no customization used.
        """

    def login_to_MySQL(self, driver = "{MySQL ODBC 8.0 Unicode Driver}", server = None, database = '', username = None, password = None, ssl_disabled=True):
        """
        Description: login to a MSSQL server and return a cursor object to query with
        Parameters:

        driver          - SQL Server Driver use {SQL Driver} or DRIVER={MySQL ODBC 8.0 Unicode Driver}
        server          - IP address of server, I.E. 127.0.0.1
        database        - Database name
        Username        - string, mysql Username
        Password        - string, mysql Password

        Return: MySQL engine
        """
        #log to console engine is created
        log.info('[MySQL engine connected...]')
        # create engine to connect to MySQL
        engine = create_engine("mysql+pymysql://" + username + ":" + password + "@" + server + "/" + database)
        # return engine to perform operations with
        return engine

    def query_MySQL_return_DataFrame(self, query, engine):
        """
        Description: query a MySQL server with a logged in cursor and
        process results into a pandas dataframe the return the dataframe.
        Parameters:

        query              - query string
        engine             - engine used to query database and load results into dataframe

        Return: pandas.DataFrame
        """

        # log to console beginning query against mssql database
        log.info('[Querying MS SQL DB...]')
        # read query into dataframe
        df = pd.read_sql(query, engine)
        # return the dataframe of results from the MySQL table
        return df

    def insert_dataframe_into_MySQL_table(self, engine, df, tablename, index = False, if_exists = 'fail'):
        """Description: attempt to insert an entire dataframe into a MySQL table
        Parameters:

        engine          - database engine
        df              - dataframe to insert into the MySQL table
        tablename       - table to insert records into
        index           - attempt to convert the index into a column to use on the insert, default to false
        if_exists       - {‘fail’, ‘replace’, ‘append’} default to 'append'
                            How to behave if the table already exists.
                            fail: Raise a ValueError.
                            replace: Drop the table before inserting new values.
                            append: Insert new values to the existing table

        return: none - insert records into mysql
        """
        # if the df column list matches the table, use all columns
        log.info('[Uploading Dataframe to MySQL DB Table...]')
        #upload records directly from dataframe using to_sql function
        df.to_sql(name = tablename, con = engine, index = index, if_exists = if_exists)

    def update_rows_in_MySQL_table(self, engine,  table_name, columns_to_update, column_values_to_update, where_column_name, where_column_list):
        """
        Description: update multiples columns in MySQL table from a dataframe on a where in list condition

        sql_update =  example:
        UPDATE <table_name>
        SET <column1_name> = <value, corresponding column1 value>, <column2_name> = <value, corresponding column2 value>,
        WHERE <Where_column_name> in < list of corresponding conditional value>;
        Parameters:

        engine                   - MySQL engine engine
        table_name               - table in MySQL to update
        columns_to_update        - column names in MySQL table to update
        column_values_to_update  - column values to upload into MySQL table
        where_column_name        - single field, condition for update
        where_column_list        - list of accepted values for where condition

        Return: None - delete records
        """
        # log to console, creating update statement to upload
        log.info('[Creating Update SQL statement...]')
        # create beginning of update, add table name
        sql_update = "UPDATE " + table_name + " SET "
        # add column names and column values to set in the update
        for index, col in enumerate(columns_to_update):
            #make sure to not grab a value outside of range and not the last row
            if index < len(columns_to_update) - 1:
                # for all lines except the last row, add column name, value, and comma
                sql_update = sql_update + col + " = '" + column_values_to_update[index] + "', "
            # adding string to sql_update for last row to update
            if index == len(columns_to_update) - 1:
                # for last row, add column name and value without a comma
                sql_update = sql_update + col + " = '" + column_values_to_update[index] + "' "
        # add where clause to end of sql string
        sql_update = sql_update + "WHERE " + where_column_name + " IN " + where_column_list + ";"

        #create connection to execute query from engine
        with engine.connect() as connection:
            #set safe mode off before update
            connection.execute(text('SET SQL_SAFE_UPDATES = 0;'))
            connection.commit()
            #execute the update of records
            connection.execute(text(sql_update))
            # commit the sql statement
            connection.commit()
            #set safe mode back on
            connection.execute(text('SET SQL_SAFE_UPDATES = 0;'))
            connection.commit()
        #log to console commiting update to table now
        log.info('[Commiting update to MySQL table...]')

    def delete_rows_in_MySQL_table(self, engine,  table_name, column_name, record_list):
        """Description: generate a query string to delete records from a MySQL table
           Parameters:

           engine                   - MySQL login connection
           table_name               - table in MySQL to update
           columns_name             - column name in MySQL table with key used to delete
           record_list              - list of key IDs to delete records

           Return: None - delete records
        """
        # Example with parameterization
        sql_delete = "DELETE FROM " + table_name + " WHERE " + column_name + " IN " + record_list + ";"

        # open connection and submit the delete query
        with engine.connect() as connection:
            #set safe mode off before update
            connection.execute(text('SET SQL_SAFE_UPDATES = 0;'))
            connection.commit()
            #execute the update of records
            connection.execute(text(sql_delete))
            # commit the sql statement
            connection.commit()
            #set safe mode back on
            connection.execute(text('SET SQL_SAFE_UPDATES = 0;'))
            connection.commit()
        #log to console commiting update to table now
        log.info('[Commiting delete to MySQL table...]')

class CS3_Utilities:
    def __init__(self):
        """Constructor Parameters:
           - currently no customization used.

           # Alternatively, using awswrangler for more robust S3 integration
           # try:
           #     df_wr = wr.s3.read_csv(path=f"s3://your-s3-bucket-name/path/to/your/data.csv")
           #     print("DataFrame from S3 (using awswrangler):")
           #     print(df_wr.head())
           # except Exception as e:
           #     print(f"Error reading CSV from S3 with awswrangler: {e}")
        """

    def create_s3_client(self, s3 = 's3'):
        return boto3.client(s3)

    def download_file_from_s3_to_local(self, bucket_name = 'your-s3-bucket-name',
                                             object_key = 'path/to/your/file.txt',
                                             local_file_path = 'downloaded_file.txt'):
        """Description: # Define your bucket name, object key (file path in S3), and local file path
        Parameters:

        Return:
        """
        try:
            s3.download_file(bucket_name, object_key, local_file_path)
            log.info(f"File '{object_key}' downloaded to '{local_file_path}' successfully.")
        except Exception as e:
            log.info(f"Error downloading file: {e}")

    def view_s3_content(self, s3, bucket_name = 'your-s3-bucket-name', object_key = 'path/to/your/text_file.txt'):
        """Description: # Define your bucket name, object key (file path in S3), and local file path
        Parameters:
        Return:
        """
        try:
            response = s3.get_object(Bucket=bucket_name, Key=object_key)
            object_content = response['Body'].read().decode('utf-8') # Decode for text files
            log.info("Content of the S3 object:")
            log.info(object_content)
        except Exception as e:
            log.info(f"Error reading object content: {e}")

    def load_csv_from_s3_to_df(self, s3_file = f"s3://your-s3-bucket-name/path/to/your/data.csv"):
        # Read CSV directly from S3 using pandas
        try:
            df = pd.read_csv(s3_file)
            log.info("DataFrame from S3:")
            log.info(df.head())
        except Exception as e:
            log.info(f"Error reading CSV from S3: {e}")
        return df

class Custom_Utilities:
    def __init__(self):
        """Constructor Parameters:
           - currently no customization used.
        """

    def merge_dfs(self, left, right, left_on, right_on, how ='inner',
                  suffixes = ('_left', '_right'), indicator = True, validate = None):
        """
        Description: merge two dataframes based on list of columns to join on
        Parameters:

        left                - left dataframe
        right               - right dataframe
        left_on             - list of string column names to perform merge on
        right_on            - list of string column names to perform merge on
        how='inner'         - what type of join to use for the merge, inner reduces duplicate the best
        suffixes            - tuple of string to append to the end of columns from each dataframe
        indicator           - indicate left/right/both dataframe the row is found in
        validate            - can check for 1:1/1:many/many:1/many:many merges

        Return: merged dataframe
        """
        # log to console merging of dataframe is occuring
        log.info('[merging dataframes...]')
        # return merged dataframe
        return pd.merge(left=left, right=right,
                        how=how, left_on=left_on, right_on=right_on,
                        suffixes=suffixes, indicator=indicator, validate=validate)

    def get_df_diffs(self, left, right, left_on, right_on, how ='inner',
                  suffixes = ('_left', '_right'), indicator = True, validate = None):
        """
        Description: merge two dataframes based on list of columns to join on,
                     then return a tuple of 3 dataframes, 1 where records exist in both left and right,
                     1 for left only and 1 for right only
        Parameters:

        left                - left dataframe
        right               - right dataframe
        left_on             - list of string column names to perform merge on
        right_on            - list of string column names to perform merge on
        how='inner'         - what type of join to use for the merge, inner reduces duplicate the best
        suffixes            - tuple of string to append to the end of columns from each dataframe
        indicator           - indicate left/right/both dataframe the row is found in
        validate            - can check for 1:1/1:many/many:1/many:many merges

        Return: tuple of three dataframes
        """
        merged_df = self.merge_dfs(left=left, right=right,
                        how=how, left_on=left_on, right_on=right_on,
                        suffixes=suffixes, indicator=indicator, validate=validate)
        both_df = merged_df[merged_df["_merge"] == "both"]

        left_only_df = merged_df[merged_df["_merge"] == "left_only"]
        right_only_df = merged_df[merged_df["_merge"] == "right_only"]
        return (both_df, left_only_df, right_only_df)

    def format_columns_dtypes(self, df):
        """
        Description: reformat dataframe columns before merge,
                     needs merge columns to be of same type,
                     honestly better to just update the data type
                     of the merging columns hardcoded instead of every column
        Parameters:

        df              - dataframe to reformat column datatypes

        Return: dataframe
        """
        log.info('[updating datatypes of dataframe...]')
        for index, col in enumerate(df.columns):
            if index < len(df.columns):
                #check if type == int
                if df[col].dtypes == 'int64':
                    print("int")
                    df[col] = df[col].astype(int)
                #check if type == string, date, or object
                if df[col].dtypes == 'object':
                    print("object")
                    df[col] = df[col].astype(str)
                #check if type == float
                if df[col].dtypes == 'float64':
                    print("float")
                    df[col] = df[col].astype(float)
                #check if type == boolean
                if df[col].dtypes == 'bool':
                    print("bool")
                    df[col] = df[col].astype(bool)

        return df

    def write_df_to_excel(self, dfs, file_name, sheet_names):
        """
        Description: Create a single excel file with multiple tabs
        Parameters:

        dfs         - list(df), list of dfs, order matters
        file_name   - string, output filename
        sheet_names - list(string), each tab name per df, order matters, must align with list of dfs

        Return: None, write dataframes to files
        """
        # create instance of ExcelWrite to add sheets creted from dataframes
        writer = pd.ExcelWriter(file_name)
        # loop through list of multiple DataFrame
        # each dataframe will be its own sheet on the document
        log.info('[writing each sheet to excelfile]')
        for index, df in enumerate(dfs):
            #log to console what sheet is being written
            log.info('[writing sheet to excel file: ' + str(sheet_names[index]) + ']')
            # write the individual dataframe to it's associated sheet
            df.to_excel(writer, sheet_names[index], index = False)
        # log finished looping and now writing file out
        log.info('[saving file to output location]')
        # save the file
        writer.save()

    def encode_df(self, df, encoding = 'unicode_escape', decoding = 'utf-8'):
        """
        Description: encode strings in unicode_escape and decode back to utf-8 for processing records as utf-8
        Parameters:

        df          - pandas.DataFrame, to encode and decode as utf-8
        encoding    - string, default to unicode_escape
        decoding    - string, default to utf-8

        Return: pandas.DataFrame
        """
        # log to console, beginning encoding data in dataframe
        log.info('[encoding query results in DataFrames]')
        # return the encoded data for strings
        return df.map(lambda x : x.encode(encoding).decode(decoding) if isinstance(x, str) else x)




    def add_sequence(self, df, group_fields, new_field, changing_fields = None, base_value = 10, increment_value = 10, sort = True):
        """
        Description: Create a new column that increments every time a group has
                     changes on a specific subgroup of fields, sorting matters,
                     This will sort the dataframe inplace.
        Parameters:

        df                      - DataFrame with the groups sorted
        group_fields            - python list of all fields to group the sequence by
        new_fields              - sequence field
        changing_fields         - optional, can declare what field is changing to compare against the group fields
        base_value              - starting value of the sequence
        increment_value         - incrementing value of the sequence
        sort                    - sort by group fields first then the changing field, changing_field must be declared

        Return: df              - DataFrame with an added column with value changing sequence
        """
        # sort the values of the data frame by the sort fields selected
        if sort:
            # log to console beginning sorted
            log.info('[Sorting DataFrame before generating list.]')
            # create list of fields to sort the dataframe by
            sort_fields = group_fields.append(changing_fields)
            # sort the dataframe based on the sort fields
            df.sort_values(sort_fields, inplace = True)
        # log to console what the new field is called that will hold the sequence
        log.info('[generating sequence for: ' + new_field + ']')
        # if there is no field declared how to sequence the group
        if changing_fields == None:
            # iterrate throught the dataframe row by row
            for index, row in df.iterrows():
                # every 10,000 rows add a log output for timekeeping
                if index % 10000 == 0:
                    # log to console a timestamp and number of rows processed
                    log.info('[rows processed: ' + str(index) + ']')
                # if the current row's group is not the same as the previous row's group
                if int(index) == 0 or not (df.loc[int(index) - 1, group_fields].equals(df.loc[index, group_fields])):
                    # start the sequence again based on the starting base value
                    df.loc[index, new_field] = base_value
                # the current row's group matches the last row's group values
                else:
                    # increment the value showing this row's group matches the previous row's group
                    df.loc[index, new_field] = df.loc[int(index) - 1, new_field] + increment_value
        # there is a declared field to sequence the group fields,
        # show what column to sequence the groups by
        else:
            # iterrate through the dataframe row by row
            for index, row in df.iterrows():
                # if on the first row
                if int(index) == 0:
                    # set the bast value to begin the sequence on
                    df.loc[index, new_field] = base_value
                # any row after the first row
                else:
                    # the current row group value is the same as the previous row, increment the sequence value
                    if (df.loc[int(index) - 1, group_fields].equals(df.loc[index, group_fields])) and not (df.loc[int(index) - 1, changing_fields].equals(df.loc[index, changing_fields])):
                        # increment the sequence value in the new column
                        df.loc[index, new_field] = df.loc[int(index) - 1, new_field] + increment_value
                    # the current row is part of a new group than the previous row
                    else:
                        # start the sequence over again on the current row
                        df.loc[index, new_field] = base_value
        # return the dataframe with the added column
        return df

    def generate_sql_list_from_df_column(self, df, column, output_file_name = None, return_line = False, output = 'file'):
        """
        Description: generate a string list of values from a dataframe column to inject into a query
        Parameters:

        df,
        column,
        output_file_name = None,
        return_line = False,
        output

        Return:     - string, sql string formatted list of values
        """

        # begin the string list that will be return by the funciont
        sql_string = "("
        unique_df = df.drop_duplicates(subset = [column])
        # loop through the rows of the dataframe to add values to the stirng
        for index, row in unique_df.iterrows():
            # if the string should start a new line after every
            if return_line:
                # add new value to the sql string on a new line every entry
                sql_string  = sql_string + "'" + str(row[column]) + "',\n"
            # generate every value of the list on a single line
            else:
                # add new value to the sql string on the same line
                sql_string  = sql_string + "'" + str(row[column]) + "',"
        # at the end of all the values added, add the closing parenthesis
        # remove ending new line if used
        if return_line:
            # remove the new line and add ending parenthesis
            sql_string = sql_string[:-2] + ")"
        # since all value are on same line, no need to trim extra new line in string
        else:
            # add ending parenthesis to sql string list
            sql_string = sql_string[:-1] + ")"
        # if the sql string is to be written to a file for further use
        if output == 'file' and output_file_name != None:
            # log to console, attempting output sql string to a file
            log.info('[Converting DataFrame Column to SQL List in text file: \n ' + output_file_name + ' ]\n')
            # open the file in write mode
            with open(output_file_name, 'w') as file:
                # write the sql string to the file and close once done
                file.write(sql_string)
        # if the sql string should be returned instead of written to file
        elif output == 'string':
            # retun sql string as list
            return sql_string
        # if don't write sql string to file, and don't return the string
        else:
            # do nothing
            return None

    def now(self, ts_format='%Y-%m-%d__%H-%M-%S'):
        """
        Description: generate a string list of values from a dataframe column to inject into a query
        Parameters:

        ts_format - default to '%Y-%m-%d__%H-%M-%S'

        Return:     - datetime of right now down to the second.
        """
        # return datetime of right now
        return datetime.fromtimestamp(time.time()).strftime(ts_format)

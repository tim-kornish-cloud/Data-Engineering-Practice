This is a mock project for migrating data setting up database to database integration through python pandas populated with mock data from https://www.mockaroo.com/

# custom_db_utilities.py
  - contains a utilities class for each database listed below to insert/delete/update/query data from multiple data sources all interfacing through pandas dataframes.
## Cloud DB
    - Salesforce
    - AWS S3
## Local DB
    - MSSQL
    - MySQL
    - MongoDB
  - Also contains an additional class with useful functions for adding data and outputting to different formats.

## Terraform
- AWS Resources mainly EC2, S3, and Lambda are all created and destroyed through terraform files in the terraform folder.

### In terraform folder use commands

```console
~$ terraform init
~$ terraform plan
~$ terraform apply
~$ terraform destroy -auto-approve
```

## Mock Data sets using Mockaroo.com

- All data is generated as csv using mockaroo, no information is from real individuals.

---

# examples for Database DML functions

---

## Salesforce
### One script for each Salesforce DML operation:
- insert records from pandas dataframe into salesforce
- update records in salesforce from pandas dataframe
- upsert records from pandas dataframe into salesforce
- delete records in salesforce from pandas dataframe

1) insert_mock_data_with_pandas_into_sf.py
2) update_mock_data_with_pandas_into_sf.py
3) upsert_mock_data_with_pandas_into_sf.py
4) delete_mock_data_with_pandas_into_sf.py

## MSSQL
### One script to perform:
- insert records from pandas dataframe into MSSQL table
- SELECT query records in MSSQL table and load into pandas dataframe
- update query records in MSSQL table from pandas dataframe
- delete query records in MSSQL table from pandas dataframe

1) INSERT_mock_data_with_pandas_into_MSSQL.py
2) SELECT_query_mock_data_from_MSSQL_into_df.py
3) UPDATE_query_mock_data_from_MSSQL_into_df.py
4) DELETE_query_csv_mock_data_from_MSSQL.py

## MySQL
### One script to:
- insert records from a pandas dataframe into a MySQL table
- update records from a pandas dataframe in a MySQL table
- delete records in a MySQL table from a pandas dataframe

1) INSERT_mock_data_with_pandas_into_MySQL.py
2) UPDATE_mock_data_with_pandas_into_MySQL.py
3) DELETE_mock_data_with_pandas_into_MySQL.py

## AWS S3
### One script to:
- load a csv into a pandas dataframe and upload into a S3 bucket
- download a csv from an S3 bucket into a pandas dataframe
- delete a csv in an s3 bucket by filename

1) load_csv_into_pandas_into_s3.py
2) download_csv_into_pandas_from_s3.py
3) delete_csv_from_s3_into_pandas.py

## MongoDB

- insert records from pandas dataframe into MongoDB database collection
- query records in MongoDB database collection and load into pandas dataframe
- update query records in MongoDB database collection from pandas dataframe
- delete query records in MongoDB database collection from pandas dataframe

1) insert_records_from_pandas_into_mongodb.py
2) load_records_into_pandas_from_mongodb.py
3) update_records_from_pandas_in_mongodb.py
4) delete_records_from_pandas_in_mongodb.py

---
# Unit Testing

in progress...


setting up database to database integration through python populated with mock data from https://www.mockaroo.com/

# custom_db_utilities.py
  - a utilities to insert/delete/update/query data from multiple data sources
    - Salesforce
    - MSSQL
    - MySQL
    - AWS S3
    - MongoDB
  - Also contains an additional class with useful functions for adding data and outputting to different formats

---

# examples for Database DML functions

---

## Salesforce

1) insert_mock_data_with_pandas_into_sf.py
2) update_mock_data_with_pandas_into_sf.py
3) upsert_mock_data_with_pandas_into_sf.py
4) delete_mock_data_with_pandas_into_sf.py

## MSSQL

1) INSERT_mock_data_with_pandas_into_MSSQL.py
2) SELECT_query_mock_data_from_MSSQL_into_df.py
3) UPDATE_query_mock_data_from_MSSQL_into_df.py
4) DELETE_query_csv_mock_data_from_MSSQL.py

## MySQL

1) INSERT_mock_data_with_pandas_into_MySQL.py
2) UPDATE_mock_data_with_pandas_into_MySQL.py
3) DELETE_mock_data_with_pandas_into_MySQL.py

## AWS S3

1) load_csv_into_pandas_into_s3.py
2) download_csv_into_pandas_from_s3.py
3) delete_csv_from_s3_into_pandas.py

## MongoDB

1) insert_records_from_pandas_into_mongodb.py
2) load_records_into_pandas_from_mongodb.py
3) update_records_from_pandas_in_mongodb.py
4) delete_records_from_pandas_in_mongodb.py

---
# Unit Testing

in progress...

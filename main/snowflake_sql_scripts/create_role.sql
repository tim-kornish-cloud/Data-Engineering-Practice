use role accountadmin;

create warehouse account_warehouse with warehouse_size = 'x-small';

create database if not exists account_db;

create role if not exists account_role;

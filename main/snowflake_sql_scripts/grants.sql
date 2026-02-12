show grants on warehouse account_warehouse;

grant usage on warehouse account_warehouse to role account_role;

grant all on database account_db to role account_role;

grant all on schema account_db.account_schema to role account_role;

GRANT ROLE account_role TO USER KORNSNOW;

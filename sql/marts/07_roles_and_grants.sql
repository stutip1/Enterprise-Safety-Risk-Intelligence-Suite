create role if not exists RAW_PII_ROLE;
create role if not exists CURATED_READ_ROLE;
create role if not exists BI_DASHBOARD_ROLE;

grant usage on database safety to role RAW_PII_ROLE;
grant usage on database safety to role CURATED_READ_ROLE;
grant usage on database safety to role BI_DASHBOARD_ROLE;

grant usage on schema raw to role RAW_PII_ROLE;
grant usage on schema staging to role CURATED_READ_ROLE;
grant usage on schema warehouse to role CURATED_READ_ROLE;
grant usage on schema marts to role BI_DASHBOARD_ROLE;

grant select on all tables in schema raw to role RAW_PII_ROLE;
grant select on all tables in schema staging to role CURATED_READ_ROLE;
grant select on all tables in schema warehouse to role CURATED_READ_ROLE;
grant select on all tables in schema marts to role BI_DASHBOARD_ROLE;

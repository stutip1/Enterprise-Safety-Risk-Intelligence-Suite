create schema if not exists audits;

create or replace masking policy audits.mask_employee_name as (val string)
returns string ->
  case
    when current_role() in ('RAW_PII_ROLE') then val
    else 'MASKED'
  end;

create or replace masking policy audits.mask_employee_id as (val string)
returns string ->
  case
    when current_role() in ('RAW_PII_ROLE') then val
    else sha2(val)
  end;

create or replace masking policy audits.mask_dob as (val date)
returns date ->
  case
    when current_role() in ('RAW_PII_ROLE') then val
    else null
  end;

alter table if exists staging.stg_employees
  modify column employee_name set masking policy audits.mask_employee_name;

alter table if exists staging.stg_employees
  modify column employee_id set masking policy audits.mask_employee_id;

alter table if exists staging.stg_employees
  modify column date_of_birth set masking policy audits.mask_dob;

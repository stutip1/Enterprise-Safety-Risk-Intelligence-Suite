create schema if not exists audits;

create or replace table audits.data_quality_log (
  audit_id string,
  audit_ts timestamp_ntz,
  dataset string,
  check_name string,
  status string,
  failure_count number(18, 0),
  details string
);

create or replace table audits.retention_log (
  audit_id string,
  audit_ts timestamp_ntz,
  dataset string,
  action string,
  retention_window_years number(4, 2),
  approver string,
  details string
);

create or replace table audits.masking_log (
  audit_id string,
  audit_ts timestamp_ntz,
  policy_name string,
  table_name string,
  column_name string,
  applied_by string,
  details string
);

insert into audits.data_quality_log
select
  uuid_string(),
  current_timestamp(),
  'staging.stg_incidents',
  'missing_incident_date',
  case when count_if(incident_ts is null) = 0 then 'PASS' else 'FAIL' end,
  count_if(incident_ts is null),
  'incident_ts must not be null'
from staging.stg_incidents;

insert into audits.data_quality_log
select
  uuid_string(),
  current_timestamp(),
  'staging.stg_hours_worked',
  'negative_hours_worked',
  case when count_if(hours_worked < 0) = 0 then 'PASS' else 'FAIL' end,
  count_if(hours_worked < 0),
  'hours_worked must be >= 0'
from staging.stg_hours_worked;

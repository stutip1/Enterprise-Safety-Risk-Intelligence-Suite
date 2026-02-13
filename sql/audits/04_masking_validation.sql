insert into audits.data_quality_log
select
  uuid_string(),
  current_timestamp(),
  'staging.stg_employees',
  'masking_policy_attached',
  case when count(*) > 0 then 'PASS' else 'FAIL' end,
  0,
  'masking policies should be attached to staging employee fields'
from table(information_schema.policy_references('staging.stg_employees'));

create schema if not exists marts;

create or replace table marts.mart_trir as
select
  dt.year,
  dt.month,
  loc.region,
  loc.site_name,
  count_if(fact.recordable_flag) as recordable_incidents,
  sum(exp.hours_worked) as hours_worked,
  (count_if(fact.recordable_flag) * 200000) / nullif(sum(exp.hours_worked), 0) as trir
from warehouse.fact_incidents fact
left join warehouse.fact_exposure exp
  on fact.location_key = exp.location_key
  and fact.date_key = exp.date_key
left join warehouse.dim_date dt on fact.date_key = dt.date_key
left join warehouse.dim_location loc on fact.location_key = loc.location_key
group by dt.year, dt.month, loc.region, loc.site_name;

create or replace table marts.mart_dart as
select
  dt.year,
  dt.month,
  loc.region,
  loc.site_name,
  count_if(fact.dart_flag) as dart_cases,
  sum(exp.hours_worked) as hours_worked,
  (count_if(fact.dart_flag) * 200000) / nullif(sum(exp.hours_worked), 0) as dart
from warehouse.fact_incidents fact
left join warehouse.fact_exposure exp
  on fact.location_key = exp.location_key
  and fact.date_key = exp.date_key
left join warehouse.dim_date dt on fact.date_key = dt.date_key
left join warehouse.dim_location loc on fact.location_key = loc.location_key
group by dt.year, dt.month, loc.region, loc.site_name;

create or replace table marts.mart_near_miss_rate as
select
  dt.year,
  dt.month,
  loc.region,
  loc.site_name,
  count(*) as near_miss_events,
  sum(exp.hours_worked) as hours_worked,
  (count(*) * 200000) / nullif(sum(exp.hours_worked), 0) as near_miss_rate
from warehouse.fact_near_miss fact
left join warehouse.fact_exposure exp
  on fact.location_key = exp.location_key
  and fact.date_key = exp.date_key
left join warehouse.dim_date dt on fact.date_key = dt.date_key
left join warehouse.dim_location loc on fact.location_key = loc.location_key
group by dt.year, dt.month, loc.region, loc.site_name;

create or replace table marts.mart_observation_rate as
select
  dt.year,
  dt.month,
  loc.region,
  loc.site_name,
  count(*) as safety_observations,
  sum(exp.hours_worked) as hours_worked,
  (count(*) * 200000) / nullif(sum(exp.hours_worked), 0) as observation_rate
from warehouse.fact_safety_observation fact
left join warehouse.fact_exposure exp
  on fact.location_key = exp.location_key
  and fact.date_key = exp.date_key
left join warehouse.dim_date dt on fact.date_key = dt.date_key
left join warehouse.dim_location loc on fact.location_key = loc.location_key
group by dt.year, dt.month, loc.region, loc.site_name;

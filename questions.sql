-- hosts
select min(host_count),
       avg(host_count),
       max(host_count),
       stddev(host_count)
  from hosts;

--  min |         avg          | max |      stddev
-- -----+----------------------+-----+------------------
--    1 | 117.8888888888888889 | 852 | 190.431956768632

select min(nagios_hosts),
       avg(nagios_hosts),
       max(nagios_hosts),
       stddev(nagios_hosts)
  from hosts;

--  min |         avg          | max |      stddev      
-- -----+----------------------+-----+------------------
--    1 | 216.4857142857142857 | 904 | 278.788383949565

   -- Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
   --  1.0    20.0    93.0   216.5   322.0   904.0


-- 20%-ile
select org_id, nagios_hosts, ntile(5) over (order by nagios_hosts) from hosts;

-- persist percentiles
alter table hosts add column quantile_5 int not null default 0;
alter table hosts add column quartile int not null default 0;
begin;
with quantile as (select org_id, ntile(20) over (order by nagios_hosts) as five from hosts)
update hosts
   set quantile_5 = q.five
  from quantile q
 where hosts.org_id = q.org_id;
update hosts set quartile = (quantile_5-1)/5 + 1;
commit;

-- distribution of alert count per week for each quartile
create table weekly as
select i.org_id,
       h.quartile,
       sum(i.hourly_count) weekly_incidents,
       sum(i.hourly_count)/h.nagios_hosts normalized_incidents
  from incidents i
  join hosts h
    on (i.org_id = h.org_id)
 group by occurrence_year, occurrence_week, i.org_id, h.nagios_hosts, h.quartile
 having count(*) > 1
 order by h.quartile, i.org_id;

create index on weekly(org_id);

create table weekly_notifying as
select i.org_id,
       h.quartile,
       sum(i.hourly_count) weekly_incidents,
       sum(i.hourly_count)/h.nagios_hosts normalized_incidents
  from incidents i
  join hosts h
    on (i.org_id = h.org_id)
 where i.auto_priority = 1
 group by occurrence_year, occurrence_week, i.org_id, h.nagios_hosts, h.quartile
 having count(*) > 1
 order by h.quartile, i.org_id;

-- notifying v. non-notifying
create table notification_ratio as
select quartile,
       1.0 * max(a) / max(b) notifying_ratio
  from (
select h.quartile,
       i.auto_priority notifying,
       case when i.auto_priority = 1 then sum(i.hourly_count) else 0 end a,
       case when i.auto_priority = 0 then sum(i.hourly_count) else 0 end b
  from incidents i
  join hosts h
    on (i.org_id = h.org_id)
 group by h.quartile, i.auto_priority) as underlying
 group by quartile;

-- worst time of day
create table worst_hour as
select (h.quantile_5 - 1)/5 + 1 as quartile,
       i.occurrence_hour as hour_of_day,
       avg(i.hourly_count) as hourly,
       max(i.hourly_count) as hourly_max,
       stddev(i.hourly_count) as hourly_stddev
  from incidents i
  join hosts h
    on (i.org_id = h.org_id)
 where i.auto_priority = 1
 group by occurrence_hour, (h.quantile_5 - 1) / 5 + 1
 having count(*) >= 1
order by quartile, hour_of_day;

-- for us
create table worst_hour_dd as
select i.occurrence_hour as hour_of_day,
       avg(i.hourly_count) as hourly,
       max(i.hourly_count) as hourly_max,
       stddev(i.hourly_count) as hourly_stddev
  from incidents i
 where i.auto_priority = 1
   and i.org_id = 2
 group by occurrence_hour
 having count(*) >= 1
order by hour_of_day;

-- worst day of week
create table worst_day_of_week as
select (h.quantile_5 - 1)/5 + 1 as quartile,
       i.occurrence_dow as day_of_week,
       24.0 * sum(i.hourly_count) / count(i.hourly_count) as daily
  from incidents i
  join hosts h
    on (i.org_id = h.org_id)
 where i.auto_priority = 1
 group by occurrence_dow, (h.quantile_5 - 1) / 5 + 1
 having count(*) >= 1
order by quartile, day_of_week;

-- for us
create table worst_day_of_week_dd as
select i.occurrence_dow as day_of_week,
       24.0 * sum(i.hourly_count) / count(i.hourly_count) as daily
  from incidents i
 where i.auto_priority = 1
   and org_id = 2
 group by occurrence_dow
 having count(*) >= 1
order by day_of_week;
  
-- distribution of alerts per hosts
create table noisiest_hosts as
with ranked as (
select quartile,
       dense_rank() over(partition by h.quartile
       		             order by 1.0 * avg(i.hourly_count) desc) rnk,
       host_name,
       avg(i.hourly_count) hourly
  from incidents i
  join hosts h
    on (i.org_id = h.org_id)
 where i.auto_priority = 1
 group by quartile, host_name
 having count(*) >= 1
order by quartile, rnk)
select quartile, rnk, hourly
  from ranked
 group by quartile, rnk, hourly
 order by quartile, rnk;

create table noisiest_hosts_no_outlier as
with ranked as (
select quartile,
       dense_rank() over(partition by h.quartile
       		             order by 1.0 * avg(i.hourly_count) desc) rnk,
       host_name,
       avg(i.hourly_count) hourly
  from incidents i
  join hosts h
    on (i.org_id = h.org_id)
 where i.auto_priority = 1
   and i.org_id <> 1000
 group by quartile, host_name
 having count(*) >= 1
order by quartile, rnk)
select quartile, rnk, hourly
  from ranked
 group by quartile, rnk, hourly
 order by quartile, rnk;

create table noisiest_hosts_outlier as
with ranked as (
select quartile,
       dense_rank() over(partition by h.quartile
       		             order by 1.0 * avg(i.hourly_count) desc) rnk,
       host_name,
       avg(i.hourly_count) hourly
  from incidents i
  join hosts h
    on (i.org_id = h.org_id)
 where i.auto_priority = 1
   and i.org_id = 1000
 group by quartile, host_name
 having count(*) >= 1
order by quartile, rnk)
select quartile, rnk, hourly
  from ranked
 group by quartile, rnk, hourly
 order by quartile, rnk;

-- Same for services
create table noisiest_checks as
with ranked as (
select quartile,
       dense_rank() over(partition by h.quartile
       		             order by 1.0 * avg(i.hourly_count) desc) rnk,
       check_name,
       avg(i.hourly_count) hourly
  from incidents i
  join hosts h
    on (i.org_id = h.org_id)
 where i.auto_priority = 1
 group by quartile, check_name
 having count(*) >= 1
order by quartile, rnk)
select quartile, rnk, hourly
  from ranked
 group by quartile, rnk, hourly
 order by quartile, rnk;

create table noisiest_checks_no_outlier as
with ranked as (
select quartile,
       dense_rank() over(partition by h.quartile
       		             order by 1.0 * avg(i.hourly_count) desc) rnk,
       check_name,
       avg(i.hourly_count) hourly
  from incidents i
  join hosts h
    on (i.org_id = h.org_id)
 where i.auto_priority = 1
   and i.org_id <> 1000
 group by quartile, check_name
 having count(*) >= 1
order by quartile, rnk)
select quartile, rnk, hourly
  from ranked
 group by quartile, rnk, hourly
 order by quartile, rnk;

create table noisiest_checks_outlier as
with ranked as (
select quartile,
       dense_rank() over(partition by h.quartile
       		             order by 1.0 * avg(i.hourly_count) desc) rnk,
       check_name,
       avg(i.hourly_count) hourly
  from incidents i
  join hosts h
    on (i.org_id = h.org_id)
 where i.auto_priority = 1
   and i.org_id = 1000
 group by quartile, check_name
 having count(*) >= 1
order by quartile, rnk)
select quartile, rnk, hourly
  from ranked
 group by quartile, rnk, hourly
 order by quartile, rnk;

-- for companies we've watched for over 1 month
-- is the survival age of alerts
-- related to its occurrence
create table survival_occurrence as
select quartile,
       (max(occurrence_doy) - min(occurrence_doy)) age_days,
       count(distinct occurrence_doy) as days_occurring
  from incidents i
  join hosts h
    on (i.org_id = h.org_id)
 where i.auto_priority = 1
   and h.observation >= interval '1 month'
 group by quartile, i.org_id, check_name
having min(occurrence_doy) < max(occurrence_doy)
 order by quartile, age_days desc;

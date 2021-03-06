-- create anonymizing function
create or replace function sha256(text) returns text as $$
select encode(digest($1::text, 'sha256'), 'hex')
$$ language sql strict immutable;

create table raw_incidents (
       org_id int not null,
       raw_date varchar not null,
       incident_type varchar not null,
       incident_level varchar not null,
       auto_priority int not null,
       check_name varchar not null,
       host_name varchar not null,
       device_name varchar,
       hourly_count int not null
);

create table incidents (
       id serial primary key,
       org_id int not null,
       occurrence_date timestamp(0) not null,
       occurrence_year int not null,
       occurrence_week int not null,
       occurrence_day int not null,
       occurrence_hour int not null,
       occurrence_dow int not null, -- 1 Monday, 7 Sunday
       occurrence_doy int not null,
       incident_type varchar not null,
       incident_level varchar not null,
       auto_priority int not null,
       check_name varchar not null,
       host_name varchar not null,
       device_name varchar,
       hourly_count int not null
);

-- find . -type f -path \*hourly\* -name \*-\* -exec psql -d nagios -c "\copy raw_incidents from '{}' with (format csv, null 'None', header false);" \;

insert into incidents
select nextval('incidents_id_seq'),
       org_id,
       (raw_date || ':00')::timestamp as occurrence_date,
       extract(year from (raw_date || ':00')::timestamp) as occurrence_year,
       extract(week from (raw_date || ':00')::timestamp) as occurrence_week,
       extract(day from (raw_date || ':00')::timestamp) as occurrence_day,
       extract(hour from (raw_date || ':00')::timestamp) as occurrence_hour,
       extract(dow from (raw_date || ':00')::timestamp) as occurrence_dow,
       extract(doy from (raw_date || ':00')::timestamp) as occurrence_doy,
       incident_type,
       incident_level,
       auto_priority,
       sha256(check_name::text) as check_name,
       sha256(host_name::text) as host_name,
       device_name,
       hourly_count
  from raw_incidents;

create index on incidents(org_id);
create index on incidents(occurrence_date);
create index on incidents(occurrence_day);
create index on incidents(occurrence_hour);
create index on incidents(occurrence_dow);
create index on incidents(incident_level);
create index on incidents(check_name);
create index on incidents(host_name);
create index on incidents(auto_priority);

create table hosts (
       org_id int primary key,
       host_count int not null
);

-- \copy hosts from org_host.csv with (format csv, header false);

create index on hosts(org_id);

-- delete useless hosts
delete from hosts where org_id in (select h.org_id from hosts h left outer join incidents i on (h.org_id = i.org_id) where i.id is null);
-- delete test accounts
delete from hosts where org_id = 1655;

create table live_hosts (
       org_id int primary key,
       host_count int not null
);

create index on live_hosts(org_id);

-- populate hosts table with distinct nagios hosts
alter table hosts add column nagios_hosts int not null default 0;

with dh as (select org_id, count(distinct(host_name)) c from incidents group by org_id)
update hosts
   set nagios_hosts = dh.c
  from dh
 where hosts.org_id = dh.org_id;

-- add observation period (earliest date, latest date)
alter table hosts add column observation interval not null default interval '0 hour';
begin;
update hosts
   set observation = (select max(occurrence_date) - min(occurrence_date)
       		        from incidents i
                       where i.org_id = hosts.org_id
		       group by i.org_id);

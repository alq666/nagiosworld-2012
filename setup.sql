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

create table hosts (
       org_id int primary key,
       host_count int not null
);

-- \copy hosts from org_host.csv with (format csv, header false);

create index on hosts(org_id);

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


-- distribution of alert count per week for each quartile
select i.org_id,
       sum(i.hourly_count) weekly_incidents,
       sum(i.hourly_count)/h.nagios_hosts normalized_incidents
  from incidents i
  join hosts h
    on (i.org_id = h.org_id)
 where h.nagios_hosts <= 20
 group by occurrence_year, occurrence_week, i.org_id, h.nagios_hosts
 having count(*) > 1
 order by i.org_id;

-- distribution of weekly alerts by nagios hosts
select h.nagios_hosts,
       sum(i.hourly_count) weekly_incidents,
       sum(i.hourly_count)/h.nagios_hosts incidents_per_host
  from incidents i
  join hosts h
    on (i.org_id = h.org_id)
 group by occurrence_year, occurrence_week, h.nagios_hosts
 order by h.nagios_hosts, incidents_per_host;


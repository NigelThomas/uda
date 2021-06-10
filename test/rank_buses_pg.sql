-- Assume we login with psql -U postgres -d demo

-- create a buses table containing only a subset of columns from the original file

drop table if exists buses cascade;

create table buses (id bigint, reported_at timestamp, lat float, lon float, speed double precision, bearing integer, highway varchar(10));

-- load from this data which is created using:
-- echo "id,reported_at,lat,lon,speed,bearing,highway" > /home/sqlstream/buses.narrow.csv
-- zcat $SQLSTREAM_HOME/demo/data/buses/30-min-at-50-rps.txt.gz | cut -d, -f1,2,8-11,14 >> /home/sqlstream/buses.narrow.csv

\copy buses from 'buses.narrow.csv' csv header;

-- should load 90,000 records

create or replace view buses_v1 as
select date_trunc('minute',reported_at) as rt
      ,case when highway is null or highway = '' then 'none' else highway end as highway
      ,id, lat, lon, speed, bearing
from buses;

create or replace view buses_v2 as
select rt,highway
	, COUNT(*) pg_count
	, PERCENTILE_DISC(0.1) within group (order by speed) as pg_pctd_10
	, PERCENTILE_CONT(0.1) within group (order by speed) as pg_pctc_10 
	, PERCENTILE_DISC(0.2) within group (order by speed) as pg_pctd_20
	, PERCENTILE_CONT(0.2) within group (order by speed) as pg_pctc_20 
	, PERCENTILE_DISC(0.7) within group (order by speed) as pg_pctd_70
	, PERCENTILE_CONT(0.7) within group (order by speed) as pg_pctc_70 
	, PERCENTILE_DISC(0.9) within group (order by speed) as pg_pctd_90
	, PERCENTILE_CONT(0.9) within group (order by speed) as pg_pctc_90 
	, array_agg(speed order by speed) as speed_array
from buses_v1
where speed is not null 
group by rt,highway
;

create or replace view percentiles as
select rt, highway
     , pg_count
     , pg_pctd_10
     , pg_pctd_20
     , pg_pctd_70
     , pg_pctd_90
     , pg_pctc_10
     , pg_pctc_20
     , pg_pctc_70
     , pg_pctc_90
     , case when pg_count < 50 then array_to_string(speed_array,',','*') else 'too large' end as speed_array
from buses_v2;

grant all on buses to demo;

grant all on percentiles to demo;

-- 2194 results

-- copy (select * from percentiles order by rt,highway) to '/tmp/percentiles.csv' csv header;

-- we should be able to compare these
-- NOTE: speed has been cast to double precision to match SQLstream implementation.

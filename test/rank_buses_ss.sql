create or replace schema percentile;
set schema 'percentile';
set path 'percentile';

create or replace foreign stream "buses"
( "id" BIGINT
, "reported_at" TIMESTAMP
, "lat" double
, "lon" double
, "speed" double
, "bearing" integer
, "highway" varchar(10)
)
SERVER FILE_SERVER
OPTIONS
( PARSER 'CSV'
, DIRECTORY '/home/sqlstream/uda/test'
, FILENAME_PATTERN 'buses.narrow.csv'
);

CREATE OR REPLACE SERVER "PostgreSQL_DB_1_buses"
    FOREIGN DATA WRAPPER "SYS_JDBC"
    OPTIONS (
        "URL" 'jdbc:postgresql://localhost/demo',
        "USER_NAME" 'demo',
        "PASSWORD" 'demodemo',
        "SCHEMA_NAME" 'public',
        "DIALECT" 'PostgreSQL',
        "JNDI_WRITEBACK" 'true',
        "pollingInterval" '1000',
        "txInterval" '1000',
        "DRIVER_CLASS" 'org.postgresql.Driver'
    );

CREATE OR REPLACE FOREIGN TABLE pg_pct_results
SERVER "PostgreSQL_DB_1_buses"
OPTIONS
( TABLE_NAME 'percentiles'
);

create or replace view tv1 
as select stream "reported_at" as ROWTIME
, floor("reported_at" to minute) as "reported_at"
, "id", "lat","lon","highway","speed"
from "buses"
where "speed" is not null;

create or replace view tv2 as
select stream "reported_at"
      , "highway"
      , count(*) as "ss_count"
      , "PERCENTILE_DISC"("speed",0.1) as "ss_pctd_10"
      , "PERCENTILE_DISC"("speed",0.2) as "ss_pctd_20"
      , "PERCENTILE_DISC"("speed",0.7) as "ss_pctd_70"
      , "PERCENTILE_DISC"("speed",0.9) as "ss_pctd_90"
      , "PERCENTILE_CONT"("speed",0.1) as "ss_pctc_10"
      , "PERCENTILE_CONT"("speed",0.2) as "ss_pctc_20"
      , "PERCENTILE_CONT"("speed",0.7) as "ss_pctc_70"
      , "PERCENTILE_CONT"("speed",0.9) as "ss_pctc_90"
from tv1 s
group by floor(s.rowtime to minute),"reported_at","highway"
order by floor(s.rowtime to minute),"reported_at","highway"
;

create or replace view tv3 as
select stream tv2.*
     , pct."pg_count"
     , pct."pg_pctd_10"
     , pct."pg_pctd_20"
     , pct."pg_pctd_70"
     , pct."pg_pctd_90"
     , pct."pg_pctc_10"
     , pct."pg_pctc_20"
     , pct."pg_pctc_70"
     , pct."pg_pctc_90"
     , pct."speed_array"
from tv2
left join pg_pct_results pct
on (trim(tv2."highway") = trim(pct."highway") AND tv2."reported_at" = pct."rt")
;

create or replace view tv4 as
select stream *
     , abs("ss_pctd_10" - "pg_pctd_10") < 0.0005 as md10
     , abs("ss_pctd_20" - "pg_pctd_20") < 0.0005 as md20
     , abs("ss_pctd_70" - "pg_pctd_70") < 0.0005 as md70
     , abs("ss_pctd_90" - "pg_pctd_90") < 0.0005 as md90
     , abs("ss_pctc_10" - "pg_pctc_10") < 0.0005 as mc10
     , abs("ss_pctc_20" - "pg_pctc_20") < 0.0005 as mc20
     , abs("ss_pctc_70" - "pg_pctc_70") < 0.0005 as mc70
     , abs("ss_pctc_90" - "pg_pctc_90") < 0.0005 as mc90
     , "ss_count" = "pg_count" as mcnt
from tv3
;

create or replace view tv5 as
select stream *
from tv4
where not (md10 and md20 and md70 and md90 and 
           mc10 and mc20 and mc70 and mc90 and
           mcnt);


select stream * from tv5;


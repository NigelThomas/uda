create or replace schema percentile;
set schema 'percentile';
set path 'percentile';

create or replace view operinfo as select stream * from stream(getStreamOperatorInfoForever(0,2));

create or replace view v1 as 
select stream *
	, count(*) over (rows unbounded preceding) total_rows
	, count(*) over (partition by node_id rows unbounded preceding) node_rows
from operinfo s;  


create or replace view pctaggview
as select stream graph_id, min(net_memory_bytes) as minmem
      , max(net_memory_bytes) as max_mem
      , "PERCENTILE_DISC"(cast(net_memory_bytes as double),0.2) as mem_20pct
from operinfo s
group by s.rowtime,graph_id;


create or replace view pctwinview
as select stream node_id, net_memory_bytes
        ,MIN(cast(net_memory_bytes as double)) over w as "min"
        ,AVG(cast(net_memory_bytes as double)) over w as "avg"
        ,MAX(cast(net_memory_bytes as double)) over w as "max"
        ,"PERCENTILE_DISC"(cast(net_memory_bytes as double),0.1) over w as mem_10pct
        ,"PERCENTILE_DISC"(cast(net_memory_bytes as double),0.2) over w as mem_20pct
        ,"PERCENTILE_DISC"(cast(net_memory_bytes as double),0.5) over w as mem_50pct
        ,"PERCENTILE_DISC"(cast(net_memory_bytes as double),0.9) over w as mem_90pct
	,"PERCENTILE_DISC"(cast(total_rows as double),0.9) over w as tr_90pct
	,"PERCENTILE_DISC"(cast(node_rows as double),0.9) over w as nr_90pct
        --,nigelUdfs."PERCENTILE_DISC"(cast(net_memory_bytes as double),2) over w as mem_200pct
from v1 s
window w as (range interval '20' second preceding)
;


-- select stream rowtime, * from pctaggview;

select stream rowtime,* from pctwinview;


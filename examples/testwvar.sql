drop schema testpct cascade;

create or replace schema testpct;
set schema 'testpct';

create or replace view operinfo as select stream * from stream(getStreamOperatorInfoForever(0,2));


create or replace view wvaraggview
as select stream graph_id,sampleUdf.wvarpop(cast(node_id as double)) as mem_wvar
from operinfo s
group by s.rowtime,graph_id;


create or replace view wcar1 as 
select stream graph_id, cast(node_id as double) as node_id
from operinfo s;


create or replace view wvarwinview as 
select stream graph_id,min(node_id) over w as minnode,max(node_id) over w as maxnode,sampleUdf.wvarpop(node_id) over w as "wvarpop"
from wcar1 s
window w  as (partition by graph_id range interval '20' second preceding) 
;

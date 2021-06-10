create or replace schema percentile;
set schema 'percentile';
set path 'percentile';

CREATE ANALYTIC FUNCTION "PERCENTILE_CONT"(x DOUBLE, fraction DOUBLE)
RETURNS DOUBLE
LANGUAGE C
PARAMETER STYLE GENERAL
NO SQL
EXTERNAL NAME 'plugin/libpctUdfs.so:percentile_contDouble';

CREATE ANALYTIC FUNCTION "PERCENTILE_DISC"(x DOUBLE, fraction DOUBLE)
RETURNS DOUBLE
LANGUAGE C
PARAMETER STYLE GENERAL
NO SQL
EXTERNAL NAME 'plugin/libpctUdfs.so:percentile_discDouble';


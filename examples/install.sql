CREATE OR REPLACE SCHEMA sampleUdf;
SET SCHEMA 'sampleUdf';
SET PATH 'sampleUdf';

create or replace function xor(i boolean, j boolean)
  returns boolean
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:xorOp';

create or replace function addLongs(i bigint, j bigint)
  returns bigint
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:addLongs';

create or replace function addMillis(t timestamp, millis bigint)
  returns timestamp
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:addLongs';
  
create or replace function addInts(i int, j int)
  returns int
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:addInts';

create or replace function addShorts(i smallint, j smallint)
  returns smallint
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:addShorts';

create or replace function addTinys(i tinyInt, j tinyInt)
  returns tinyInt
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:addTinys';

create or replace function addDoubles(i double, j double)
  returns double
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:addDoubles';

create or replace function addFloats(i float, j float)
  returns float
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:addDoubles';

create or replace function negateLongOrNull(i bigint)
  returns bigint
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:negateLong';

-- create or replace function concat(x varchar(100), y varchar(100))
--   returns varchar(200)
--   language c
--   parameter style general
--   no sql
--   external name 'plugin/libsampleUdfs.so:concatVC';

create or replace function addSome(w double, x smallint, y int, z bigint)
  returns double
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:addSome';

create or replace function varbinary2varchar(x varbinary(100))
  returns varchar(5)
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:varbinary2varchar';

create or replace function varbinary2char(x varbinary(100))
  returns char(5)
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:varbinary2char';

create or replace function varbinary2varbinary(x varbinary(100))
  returns varbinary(5)
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:varbinary2varbinary';

create or replace function varbinary2binary(x varbinary(100))
  returns binary(5)
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:varbinary2binary';

create or replace function unicodeLength(x varchar(100))
  returns int
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:unicodeLength';

-- Install sample aggregate functions


create or replace aggregate function mySum(i int)
  returns int
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:myAdd';

create or replace analytic function wVarPop(x double)
  returns double
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:welfordPopVariance';

create or replace analytic function wVarPop(x double)
  returns double
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:welfordPopVariance';

create or replace analytic function wVarSamp(x double)
  returns double
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:welfordSampVariance';

create or replace analytic function wMean(x double)
  returns double
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:welfordMean';

create or replace analytic function wCount(x double)
  returns bigint
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:welfordCount';

create or replace analytic function cwVarPop(x double)
  returns double
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:cwelfordPopVariance';

create or replace analytic function cwMean(x double)
  returns double
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:cwelfordMean';

create or replace analytic function cwCount(x double)
  returns bigint
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:cwelfordCount';


create or replace aggregate function strlist(s varchar(30))
  returns varchar(30)
  language c
  parameter style general
  no sql
  external name 'plugin/libsampleUdfs.so:strlist';



#!/bin/bash
#
# make data for percentile test
. /etc/sqlstream/environment

: ${HERE:=`dirname $0`}

echo "id,reported_at,lat,lon,speed,bearing,highway" > $HERE/buses.narrow.csv
zcat $SQLSTREAM_HOME/demo/data/buses/30-min-at-50-rps.txt.gz | cut -d, -f1,2,8-11,14 >> $HERE/buses.narrow.csv


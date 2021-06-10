# Testing the PERCENTILE_DISC function

Run these tests on a `sqlstream/complete` container.

We use a narrow CSV version of the buses data, created like so:

```
echo "id,reported_at,lat,lon,speed,bearing,highway" > /home/sqlstream/buses.narrow.csv
zcat $SQLSTREAM_HOME/demo/data/buses/30-min-at-50-rps.txt.gz | cut -d, -f1,2,8-11,14 >> /home/sqlstream/buses.narrow.csv
```

We load the data into Postgres using the `rank_buses_pg.sql` script

```
psql -U postgres -d demo -f rank_buses_pg.sql
```

This creates a file `/tmp/percentiles.csv` as reference results. We are relying on the PostgreSQL team to have validated their results.

Next we run the equivalent query against the streaming buses data. We need to use the same narrow version of the buses data to ensure we are comparing like with like.



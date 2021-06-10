./build.sh

cp build/plugin/libpctUdfs.so $SQLSTREAM_HOME/plugin
sudo service s-serverd restart

# sqllineClient --run=install.sql
sqllineClient --run=pctinstall.sql



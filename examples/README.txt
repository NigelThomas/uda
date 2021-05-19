This example demonstrates how to build and install a number
of user defined functions and user defined aggregates.

Prerequisites:
Install cmake and gcc-8.

On Ubuntu this would be done with:

sudo apt-get install cmake gcc-8 g++-8
export CC=/usr/bin/gcc-8
export CXX=/usr/bin/g++-8

Build shared library:

./build.sh

This will create build/plugin/libsampleUdfs.so

Install shared library:

copy the newly created libsampleUdfs.so to plugin directory of s-Server directory.

Then invoke sqlline --run=install.sql

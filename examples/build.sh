#!/bin/sh
cd `dirname $0`
examplesDir=`pwd`
mkdir -p ${examplesDir}/build
cd ${examplesDir}/build
echo "${examplesDir}/../include"
cmake -Dinclude_dir="${examplesDir}/../include" -Dfennel_opt_flag='with-optimization' ../src
make

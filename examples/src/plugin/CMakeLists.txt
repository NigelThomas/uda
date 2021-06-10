# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

add_library(
sampleUdfs SHARED
    sampleUdfs.cpp
)

add_library(
pctUdfs SHARED
    percentile.cpp
)


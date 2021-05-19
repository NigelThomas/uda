# $Id$

cmake_minimum_required(VERSION 2.6)
project(aspen)

if ("${fennel_opt_flag}" MATCHES "--with-optimization")
   set(CMAKE_BUILD_TYPE Release)
   set(AKA_SECURITY "-Wformat -Wformat-security -fstack-protector --param ssp-buffer-size=4 -D_FORTIFY_SOURCE=2 -Wl,-z,relro")
else()
   set(CMAKE_BUILD_TYPE Debug)
   set(AKA_SECURITY "-Wformat -Wformat-security -fstack-protector --param ssp-buffer-size=4 -Wl,-z,relro")
 endif()
 
set(CMAKE_CXX_FLAGS "${AKA_SECURITY} -fPIC -DPIC -std=gnu++17")
 
set(
  DEFAULT_INCLUDE
  ${include_dir}
)

set(
DEFAULT_LIB
)

add_subdirectory(plugin)

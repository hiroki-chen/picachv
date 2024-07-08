cmake_minimum_required(VERSION 3.20)
project(tpch)

set(CMAKE_CXX_STANDARD 17)
set(DUCKDB_PATH ~/duckdb)

include_directories(${DUCKDB_PATH}/src/include ~/picachv/picachv-api/c_headers)

add_executable(tpch main.cc queries.cc)
target_link_libraries(tpch duckdb messages)
if ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
  target_link_directories(tpch PUBLIC ${DUCKDB_PATH}/build/release/src ${DUCKDB_PATH}/build/release/src/messages)
else()
  target_link_directories(tpch PUBLIC ${DUCKDB_PATH}/build/debug/src ${DUCKDB_PATH}/build/debug/src/messages)
endif()

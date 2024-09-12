#ifndef _PICACHV_DUCKDB_QUERIES_H_
#define _PICACHV_DUCKDB_QUERIES_H_

#include <memory>
#include <string>

#include "cxxopts.h"
#include "duckdb.hpp"

// Defines all the table names used in the TPC-H queries.
static const std::string kTableNames[] = {"lineitem", "orders",   "part",
                                          "supplier", "customer", "partsupp",
                                          "nation",   "region"};

static const int kTableNum = sizeof(kTableNames) / sizeof(kTableNames[0]);

struct QueryStat {
  bool success;
  std::chrono::duration<double> time;
};

class QueryFactory {
  std::optional<std::string> policy_path_;
  uint32_t thread_num_;
  std::string data_path_;
  bool enable_profiling_;
  int query_num_;

  std::unique_ptr<duckdb::Connection> con_;

private:
  bool PrepareTable(const std::string &table_name);

  QueryStat ExecuteQuery1();
  QueryStat ExecuteQuery2();
  QueryStat ExecuteQuery3();
  QueryStat ExecuteQuery4();
  QueryStat ExecuteQuery5();
  QueryStat ExecuteQuery6();
  QueryStat ExecuteQuery7();
  QueryStat ExecuteQuery8();
  QueryStat ExecuteQuery9();
  QueryStat ExecuteQuery10();
  QueryStat ExecuteQuery11();
  QueryStat ExecuteQuery12();
  QueryStat ExecuteQuery13();
  QueryStat ExecuteQuery14();
  // QueryStat ExecuteQuery15();
  QueryStat ExecuteQuery16();
  QueryStat ExecuteQuery17();
  QueryStat ExecuteQuery18();
  QueryStat ExecuteQuery19();
  QueryStat ExecuteQuery20();
  QueryStat ExecuteQuery21();
  QueryStat ExecuteQuery22();

  QueryStat ExecuteQueryInternal(const std::string &query);

public:
  QueryFactory(cxxopts::ParseResult &options);

  bool Setup(std::unique_ptr<duckdb::Connection> con);

  QueryStat ExecuteQuery();
};

#endif // _PICACHV_DUCKDB_QUERIES_H_

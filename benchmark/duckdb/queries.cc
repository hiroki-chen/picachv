#include <chrono>
#include <iostream>
#include <optional>

#include "picachv_interfaces.h"
#include "queries.h"

QueryStat QueryFactory::ExecuteQueryInternal(const std::string &query) {
  bool prev = con_->PolicyCheckingEnabled();
  if (prev) {
    con_->DisablePolicyChecking();
  }
  auto desc = con_->Query("EXPLAIN(" + query + ")");
  desc->Print();
  if (prev) {
    con_->EnablePolicyChecking();
  }

  auto start = std::chrono::high_resolution_clock::now();
  auto result = con_->Query(query);
  auto end = std::chrono::high_resolution_clock::now();
  result->Print();

  if (result->HasError()) {
    std::cerr << "Query 2 failed:\n\t";
    result->Print();
    return QueryStat{.success = false, .time = end - start};
  }

  return QueryStat{.success = true, .time = end - start};
}

QueryFactory::QueryFactory(cxxopts::ParseResult &options) {

  if (options.count("policy-path")) {
    policy_path_ = options["policy-path"].as<std::string>();
  } else {
    policy_path_ = std::nullopt;
  }

  if (!options.count("query-num")) {
    std::cerr << "Please specify the query number!" << std::endl;
    exit(1);
  }

  if (!options.count("data-path")) {
    std::cerr << "Please specify the data path!" << std::endl;
    exit(1);
  }

  data_path_ = options["data-path"].as<std::string>();
  enable_profiling_ = options["enable-profiling"].as<bool>();
  query_num_ = options["query-num"].as<int>();
  thread_num_ = options["thread-num"].as<uint32_t>();
}

bool QueryFactory::PrepareTable(const std::string &table_name) {
  if (!policy_path_.has_value()) {
    return true;
  }

  const std::string policy_path =
      data_path_ + "/" + policy_path_.value() + table_name + ".parquet.bin";

  // Need to figure a way out to load the policy.
  // Possible solution: parquet_extension.cpp

  return true;
}

bool QueryFactory::Setup(std::unique_ptr<duckdb::Connection> con) {
  con_ = std::move(con);

  if (thread_num_ > 0) {
    std::cout << "Setting the number of threads to " << thread_num_
              << std::endl;
    con_->Query("SET threads TO " + std::to_string(thread_num_));
  }

  if (policy_path_.has_value()) {
    // Set up the context.
    ErrorCode err = con_->InitializeCtx();
    if (err != ErrorCode::Success) {
      std::cerr << "Failed to initialize the context: " << err << std::endl;
      return false;
    }

    con_->EnablePolicyChecking();

    if (enable_profiling_) {
      con_->EnableProfiling();
      con_->EnablePicachvProfiling();
    }

    // Register policies.
    for (size_t i = 0; i < kTableNum; i++) {
      const std::string table_path =
          data_path_ + "/" + kTableNames[i] + ".parquet";
      const std::string policy_path =
          policy_path_.value() + kTableNames[i] + ".parquet.policy.parquet";

      std::cout << "table_path: " << table_path << std::endl;
      std::cout << "policy_path: " << policy_path << std::endl;

      err = con_->RegisterPolicyParquet(table_path, policy_path);
      if (err != ErrorCode::Success) {
        std::cerr << "Failed to register the policy: " << err << std::endl;
        return false;
      }
    }
  }

  return true;
}

QueryStat QueryFactory::ExecuteQuery() {
  switch (query_num_) {
  case 1:
    return ExecuteQuery1();
  case 2:
    return ExecuteQuery2();
  case 3:
    return ExecuteQuery3();
  case 4:
    return ExecuteQuery4();
  case 5:
    return ExecuteQuery5();
  case 6:
    return ExecuteQuery6();
  case 7:
    return ExecuteQuery7();
  default:
    return QueryStat{.success = false,
                     .time = std::chrono::duration<double>(0)};
  }
}

QueryStat QueryFactory::ExecuteQuery1() {
  const std::string lineitem = data_path_ + "/" + kTableNames[0] + ".parquet";

  if (!PrepareTable(kTableNames[0])) {
    std::cerr << "Failed to prepare the table: " << kTableNames[0] << std::endl;
    return QueryStat{.success = false,
                     .time = std::chrono::duration<double>(0)};
  }

  // Query 1
  std::string query =
      "SELECT l_returnflag, l_linestatus, "
      "sum(l_quantity) as sum_qty, "
      "sum(l_extendedprice) as sum_base_price, "
      "sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, "
      "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, "
      "avg(l_quantity) as avg_qty, "
      "avg(l_extendedprice) as avg_price, "
      "avg(l_discount) as avg_disc, "
      "count(*) as count_order "
      "FROM '" +
      lineitem +
      "' "
      // "WHERE l_shipdate <= '1998-12-01' "
      "GROUP BY l_returnflag, l_linestatus "
      "ORDER BY l_returnflag, l_linestatus";

  bool prev = con_->PolicyCheckingEnabled();
  if (prev) {
    con_->DisablePolicyChecking();
  }
  auto desc = con_->Query("EXPLAIN(" + query + ")");
  desc->Print();
  if (prev) {
    con_->EnablePolicyChecking();
  }

  auto start = std::chrono::high_resolution_clock::now();
  auto result = con_->Query(query);
  auto end = std::chrono::high_resolution_clock::now();
  result->Print();

  if (result->HasError()) {
    std::cerr << "Query 1 failed:\n\t";
    result->Print();
    return QueryStat{.success = false, .time = end - start};
  }

  return QueryStat{.success = true, .time = end - start};
}

QueryStat QueryFactory::ExecuteQuery2() {
  const std::string part = data_path_ + "/" + kTableNames[2] + ".parquet";
  const std::string supplier = data_path_ + "/" + kTableNames[3] + ".parquet";
  const std::string partsupp = data_path_ + "/" + kTableNames[5] + ".parquet";
  const std::string nation = data_path_ + "/" + kTableNames[6] + ".parquet";
  const std::string region = data_path_ + "/" + kTableNames[7] + ".parquet";

  std::string sub_query =
      "select min(ps_supplycost) as min_supplycost "
      "from '" +
      part + "', '" + supplier + "', '" + partsupp + "', '" + nation + "', '" +
      region +
      "' "
      "where p_partkey = ps_partkey and s_suppkey = ps_suppkey "
      "and p_size = 15 "
      "and p_type like '%BRASS' "
      "and s_nationkey = n_nationkey "
      "and n_regionkey = r_regionkey "
      "and r_name = 'EUROPE'";
  std::string query = "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, "
                      "s_address, s_phone, s_comment "
                      "from '" +
                      part + "', '" + supplier + "', '" + partsupp + "', '" +
                      nation + "', '" + region +
                      "' "
                      "where p_partkey = ps_partkey and s_suppkey = ps_suppkey "
                      "and p_size = 15 "
                      "and p_type like '%BRASS' "
                      "and s_nationkey = n_nationkey "
                      "and n_regionkey = r_regionkey "
                      "and r_name = 'EUROPE' "
                      "and ps_supplycost = (" +
                      sub_query +
                      ") "
                      "order by s_acctbal desc, n_name, s_name, p_partkey "
                      "limit 100";

  return ExecuteQueryInternal(query);
}

QueryStat QueryFactory::ExecuteQuery3() {
  const std::string customer = data_path_ + "/" + kTableNames[4] + ".parquet";
  const std::string orders = data_path_ + "/" + kTableNames[1] + ".parquet";
  const std::string lineitem = data_path_ + "/" + kTableNames[0] + ".parquet";

  std::string query =
      "SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, "
      "o_orderdate, o_shippriority "
      "FROM '" +
      customer + "', '" + orders + "', '" + lineitem +
      "' "
      "WHERE c_mktsegment = 'BUILDING' "
      "and c_custkey = o_custkey "
      "and l_orderkey = o_orderkey "
      "and l_shipdate > '1995-03-15' "
      "and l_shipdate < '1995-03-25' "
      "GROUP BY l_orderkey, o_orderdate, o_shippriority "
      "ORDER BY revenue desc, o_orderdate "
      "LIMIT 10";

  return ExecuteQueryInternal(query);
}

QueryStat QueryFactory::ExecuteQuery4() {
  const std::string lineitem = data_path_ + "/" + kTableNames[0] + ".parquet";
  const std::string orders = data_path_ + "/" + kTableNames[1] + ".parquet";

  std::string sub_query = "select * "
                          "from '" +
                          lineitem +
                          "' "
                          "where l_commitdate < l_receiptdate";

  std::string query = "select o_orderpriority, count(*) as order_count "
                      "from '" +
                      orders +
                      "' "
                      "where o_orderdate >= '1993-07-01' "
                      "and o_orderdate < '1993-10-01' "
                      "and exists (" +
                      sub_query +
                      ") "
                      "group by o_orderpriority "
                      "order by o_orderpriority";

  return ExecuteQueryInternal(query);
}

QueryStat QueryFactory::ExecuteQuery5() {
  const std::string customer = data_path_ + "/" + kTableNames[4] + ".parquet";
  const std::string orders = data_path_ + "/" + kTableNames[1] + ".parquet";
  const std::string lineitem = data_path_ + "/" + kTableNames[0] + ".parquet";
  const std::string supplier = data_path_ + "/" + kTableNames[3] + ".parquet";
  const std::string nation = data_path_ + "/" + kTableNames[6] + ".parquet";
  const std::string region = data_path_ + "/" + kTableNames[7] + ".parquet";

  std::string query =
      "select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue "
      "from '" +
      customer + "', '" + orders + "', '" + lineitem + "', '" + supplier +
      "', '" + nation + "', '" + region +
      "' "
      "where c_custkey = o_custkey "
      "and l_orderkey = o_orderkey "
      "and l_suppkey = s_suppkey "
      "and c_nationkey = s_nationkey "
      "and s_nationkey = n_nationkey "
      "and n_regionkey = r_regionkey "
      "and r_name = 'ASIA' "
      "and o_orderdate >= '1994-01-01' "
      "and o_orderdate < '1995-01-01' "
      "group by n_name "
      "order by revenue desc";
  ;

  return ExecuteQueryInternal(query);
}

QueryStat QueryFactory::ExecuteQuery6() {
  const std::string lineitem = data_path_ + "/" + kTableNames[0] + ".parquet";

  std::string query = "select sum(l_extendedprice * l_discount) as revenue "
                      "from '" +
                      lineitem +
                      "' "
                      "where l_shipdate >= '1994-01-01' "
                      "and l_shipdate < '1995-01-01' "
                      "and l_discount between 0.06 - 0.01 and 0.06 + 0.01 "
                      "and l_quantity < 24";

  return ExecuteQueryInternal(query);
}

QueryStat QueryFactory::ExecuteQuery7() {
  const std::string supplier = data_path_ + "/" + kTableNames[3] + ".parquet";
  const std::string lineitem = data_path_ + "/" + kTableNames[0] + ".parquet";
  const std::string orders = data_path_ + "/" + kTableNames[1] + ".parquet";
  const std::string customer = data_path_ + "/" + kTableNames[4] + ".parquet";
  const std::string nation = data_path_ + "/" + kTableNames[6] + ".parquet";

  std::string sub_query =
      "select n1.name as supp_nation, n2.name as cust_nation, "
      "extract(year from l_shipdate) as l_year, "
      "l_extendedprice * (1 - l_discount) as volume "
      "from '" +
      supplier + "', '" + lineitem + "', '" + orders + "', '" + customer +
      "', '" + nation +
      "' "
      "where s_suppkey = l_suppkey "
      "and o_orderkey = l_orderkey "
      "and c_custkey = o_custkey "
      "and s_nationkey = n1.nationkey "
      "and c_nationkey = n2.nationkey "
      "and ("
      "(n1.name = 'FRANCE' and n2.name = 'GERMANY') or "
      "(n1.name = 'GERMANY' and n2.name = 'FRANCE') "
      ")";

  std::string query =
      "select supp_nation, cust_nation, l_year, sum(volume) as revenue "
      "from (" +
      sub_query +
      ") as shipping "
      "group by supp_nation, cust_nation, l_year "
      "order by supp_nation, cust_nation, l_year";

  return ExecuteQueryInternal(query);
}
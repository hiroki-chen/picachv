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
  // A simple multi-threading setting causes deadlock on the rayon side???.
  // con_->Query("SET threads TO 2");

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

  con_->Query("SET threads TO 1");

  std::string sub_query =
      "SELECT min(ps_supplycost) FROM '" + partsupp + "', '" + supplier +
      "', '" + nation + "', '" + region + "', '" + part +
      "' "
      "WHERE p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey "
      "= n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE'";

  // std::string query =
  //     "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, "
  //     "s_phone, s_comment "
  //     "from '" +
  //     part + "', '" + supplier + "', '" + partsupp + "', '" + nation + "', '"
  //     + region +
  //     "' "
  //     "where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size =
  //     15 " "and p_type like '%BRASS' " "and s_nationkey = n_nationkey and
  //     n_regionkey = r_regionkey and r_name "
  //     "= 'EUROPE' "
  //     "and ps_supplycost = (" +
  //     sub_query +
  //     ") "
  //     "order by s_acctbal desc, n_name, s_name, p_partkey "
  //     "limit 100";

  return ExecuteQueryInternal(sub_query);
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

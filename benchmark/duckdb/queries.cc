#include <chrono>
#include <optional>

#include "picachv_interfaces.h"
#include "queries.h"

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
      "ORDER BY l_returnflag, l_linestatus;";

  auto start = std::chrono::high_resolution_clock::now();
  auto result = con_->Query(query);
  auto end = std::chrono::high_resolution_clock::now();

  if (result->HasError()) {
    std::cerr << "Query 1 failed:\n\t";
    result->Print();
    return QueryStat{.success = false, .time = end - start};
  }

  return QueryStat{.success = true, .time = end - start};
}

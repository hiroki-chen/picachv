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
    std::cerr << "Query failed:\n\t";
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
  case 8:
    return ExecuteQuery8();
  case 9:
    return ExecuteQuery9();
  case 10:
    return ExecuteQuery10();
  case 11:
    return ExecuteQuery11();
  case 12:
    return ExecuteQuery12();
  case 13:
    return ExecuteQuery13();
  case 14:
    return ExecuteQuery14();
  case 16:
    return ExecuteQuery16();
  case 17:
    return ExecuteQuery17();
  case 18:
    return ExecuteQuery18();
  case 19:
    return ExecuteQuery19();
  case 20:
    return ExecuteQuery20();
  default:
    std::cerr << "no such query: " << query_num_ << std::endl;
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
      "select n1.n_name as supp_nation, n2.n_name as cust_nation, "
      "extract(year from l_shipdate) as l_year, "
      "l_extendedprice * (1 - l_discount) as volume "
      "from '" +
      supplier + "', '" + lineitem + "', '" + orders + "', '" + customer +
      "', '" + nation + "' as n1 " + ", '" + nation +
      "' as n2 "
      "where s_suppkey = l_suppkey "
      "and o_orderkey = l_orderkey "
      "and c_custkey = o_custkey "
      "and s_nationkey = n1.n_nationkey "
      "and c_nationkey = n2.n_nationkey "
      "and ("
      "(n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or "
      "(n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE') "
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

// PASSED.
QueryStat QueryFactory::ExecuteQuery8() {
  const std::string part = data_path_ + "/" + kTableNames[2] + ".parquet";
  const std::string supplier = data_path_ + "/" + kTableNames[3] + ".parquet";
  const std::string lineitem = data_path_ + "/" + kTableNames[0] + ".parquet";
  const std::string orders = data_path_ + "/" + kTableNames[1] + ".parquet";
  const std::string customer = data_path_ + "/" + kTableNames[4] + ".parquet";
  const std::string nation = data_path_ + "/" + kTableNames[6] + ".parquet";
  const std::string region = data_path_ + "/" + kTableNames[7] + ".parquet";

  std::string sub_query =
      "select extract(year from o_orderdate) as o_year, "
      "l_extendedprice * (1 - l_discount) as volume, "
      "n2.n_name as nation "
      "from '" +
      part + "', '" + supplier + "', '" + lineitem + "', '" + orders + "', '" +
      customer + "', '" + nation + "' n1, '" + nation + "' n2, '" + region +
      "' "
      "where p_partkey = l_partkey "
      "and s_suppkey = l_suppkey "
      "and l_orderkey = o_orderkey "
      "and o_custkey = c_custkey "
      "and c_nationkey = n1.n_nationkey "
      "and n1.n_regionkey = r_regionkey "
      "and r_name = 'AMERICA' "
      "and s_nationkey = n2.n_nationkey "
      "and o_orderdate between '1995-01-01' and '1996-12-31' "
      "and p_type = 'ECONOMY ANODIZED STEEL'";

  std::string query = "select o_year, sum(case "
                      "when nation = 'BRAZIL' then volume "
                      "else 0.0 "
                      "end) / sum(volume) as mkt_share "
                      "from (" +
                      sub_query +
                      ") as all_nations "
                      "group by o_year "
                      "order by o_year";

  return ExecuteQueryInternal(query);
}

QueryStat QueryFactory::ExecuteQuery9() {
  const std::string part = data_path_ + "/" + kTableNames[2] + ".parquet";
  const std::string supplier = data_path_ + "/" + kTableNames[3] + ".parquet";
  const std::string lineitem = data_path_ + "/" + kTableNames[0] + ".parquet";
  const std::string partsupp = data_path_ + "/" + kTableNames[5] + ".parquet";
  const std::string orders = data_path_ + "/" + kTableNames[1] + ".parquet";
  const std::string nation = data_path_ + "/" + kTableNames[6] + ".parquet";

  std::string sub_query =
      "select n_name as nation, extract(year from o_orderdate) as o_year, "
      "l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as "
      "amount "
      "from '" +
      part + "', '" + supplier + "', '" + lineitem + "', '" + partsupp +
      "', '" + orders + "', '" + nation +
      "' "
      "where s_suppkey = l_suppkey "
      "and ps_suppkey = l_suppkey "
      "and ps_partkey = l_partkey "
      "and p_partkey = l_partkey "
      "and o_orderkey = l_orderkey "
      "and s_nationkey = n_nationkey "
      "and p_name LIKE '\%green\%'";

  std::string query = "select nation, o_year, sum(amount) as sum_profit "
                      "from (" +
                      sub_query +
                      ") as profit "
                      "group by nation, o_year "
                      "order by nation, o_year desc";

  return ExecuteQueryInternal(query);
}

QueryStat QueryFactory::ExecuteQuery10() {
  const std::string customer = data_path_ + "/" + kTableNames[4] + ".parquet";
  const std::string orders = data_path_ + "/" + kTableNames[1] + ".parquet";
  const std::string lineitem = data_path_ + "/" + kTableNames[0] + ".parquet";
  const std::string nation = data_path_ + "/" + kTableNames[6] + ".parquet";

  std::string query =
      "select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as "
      "revenue, c_acctbal, n_name, c_address, c_phone, c_comment "
      "from '" +
      customer + "', '" + orders + "', '" + lineitem + "', '" + nation +
      "' "
      "where c_custkey = o_custkey "
      "and l_orderkey = o_orderkey "
      "and o_orderdate >= '1993-10-01' "
      "and o_orderdate < '1994-01-01' "
      "and l_returnflag = 'R' "
      "and c_nationkey = n_nationkey "
      "group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, "
      "c_comment "
      "order by revenue desc";

  return ExecuteQueryInternal(query);
}

// FIXME: Stuck.
QueryStat QueryFactory::ExecuteQuery11() {
  const std::string partsupp = data_path_ + "/" + kTableNames[5] + ".parquet";
  const std::string supplier = data_path_ + "/" + kTableNames[3] + ".parquet";
  const std::string nation = data_path_ + "/" + kTableNames[6] + ".parquet";

  std::string sub_query = "select sum(ps_supplycost * ps_availqty) * 0.0001 "
                          "from '" +
                          partsupp + "', '" + supplier + "', '" + nation +
                          "' "
                          "where ps_suppkey = s_suppkey "
                          "and s_nationkey = n_nationkey "
                          "and n_name = 'GERMANY'";

  std::string query =
      "select ps_partkey, sum(ps_supplycost * ps_availqty) as value "
      "from '" +
      partsupp + "', '" + supplier + "', '" + nation +
      "' "
      "where ps_suppkey = s_suppkey "
      "and s_nationkey = n_nationkey "
      "and n_name = 'GERMANY' "
      "group by ps_partkey "
      "having sum(ps_supplycost * ps_availqty) > (" +
      sub_query +
      ") "
      "order by value desc";

  return ExecuteQueryInternal(query);
}

// PASSED
QueryStat QueryFactory::ExecuteQuery12() {
  const std::string orders = data_path_ + "/" + kTableNames[1] + ".parquet";
  const std::string lineitem = data_path_ + "/" + kTableNames[0] + ".parquet";

  std::string query =
      "select l_shipmode, sum(case "
      "when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' "
      "then 1 "
      "else 0 "
      "end) as high_line_count, "
      "sum(case "
      "when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' "
      "then 1 "
      "else 0 "
      "end) as low_line_count "
      "from '" +
      orders + "', '" + lineitem +
      "' "
      "where o_orderkey = l_orderkey "
      "and l_shipmode in ('MAIL', 'SHIP') "
      "and l_commitdate < l_receiptdate "
      "and l_shipdate < l_commitdate "
      "and l_receiptdate >= '1994-01-01' "
      "and l_receiptdate < '1995-01-01' "
      "group by l_shipmode "
      "order by l_shipmode";

  return ExecuteQueryInternal(query);
}

QueryStat QueryFactory::ExecuteQuery13() {
  const std::string customer = data_path_ + "/" + kTableNames[4] + ".parquet";
  const std::string orders = data_path_ + "/" + kTableNames[1] + ".parquet";

  std::string query = "select c_count, count(*) as custdist "
                      "from ( "
                      "select c_custkey, count(o_orderkey) as c_count "
                      "from '" +
                      customer + "', '" + orders +
                      "' " // due to technical limitation we don't use outer
                           // join here; but can be implemented.
                      "where c_custkey = o_custkey "
                      "and o_comment not like '%special%requests%' "
                      "group by c_custkey "
                      ") as c_orders (c_custkey, c_count)"
                      "group by c_count "
                      "order by custdist desc, c_count desc";

  return ExecuteQueryInternal(query);
}

// CASE WHEN.
QueryStat QueryFactory::ExecuteQuery14() {
  const std::string linitem = data_path_ + "/" + kTableNames[0] + ".parquet";
  const std::string part = data_path_ + "/" + kTableNames[2] + ".parquet";

  std::string query =
      "select 100.00 * sum(case "
      "when p_type like 'PROMO%' "
      "then l_extendedprice * (1 - l_discount) "
      "else 0 "
      "end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue "
      "from '" +
      linitem + "', '" + part +
      "' "
      "where l_partkey = p_partkey "
      "and l_shipdate >= '1995-09-01' "
      "and l_shipdate < '1995-10-01'";

  return ExecuteQueryInternal(query);
}

// FIXME: Many bugs.
QueryStat QueryFactory::ExecuteQuery16() {
  const std::string partsupp = data_path_ + "/" + kTableNames[5] + ".parquet";
  const std::string part = data_path_ + "/" + kTableNames[2] + ".parquet";
  const std::string supplier = data_path_ + "/" + kTableNames[3] + ".parquet";

  std::string sub_query = "select s_suppkey "
                          "from '" +
                          supplier +
                          "' "
                          "where s_comment like '%Customer%Complaints%'";

  std::string query =
      "select p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt "
      "from '" +
      partsupp + "', '" + part +
      "' "
      "where p_partkey = ps_partkey "
      "and p_brand <> 'Brand#45' "
      "and p_type not like 'MEDIUM POLISHED%' "
      // "and p_size in (49, 14, 23, 45, 19, 3, 36, 9) "
      "and ps_suppkey not in (" +
      sub_query +
      ") "
      "group by p_brand, p_type, p_size ";
  // "order by supplier_cnt desc, p_brand, p_type, p_size";

  return ExecuteQueryInternal(query);
}

// TODO: RIGHT_DELIM_JOIN. WTF is this.
QueryStat QueryFactory::ExecuteQuery17() {
  const std::string lineitem = data_path_ + "/" + kTableNames[0] + ".parquet";
  const std::string part = data_path_ + "/" + kTableNames[2] + ".parquet";

  std::string sub_query = "select 0.2 * avg(l_quantity) "
                          "from '" +
                          lineitem +
                          "' "
                          "where l_partkey = p_partkey";

  std::string query = "select sum(l_extendedprice) / 7.0 as avg_yearly "
                      "from '" +
                      lineitem + "', '" + part +
                      "' "
                      "where p_partkey = l_partkey "
                      "and p_brand = 'Brand#23' "
                      "and p_container = 'MED BOX' "
                      "and l_quantity < (" +
                      sub_query + ")";

  return ExecuteQueryInternal(query);
}

// PASSED
QueryStat QueryFactory::ExecuteQuery18() {
  const std::string customer = data_path_ + "/" + kTableNames[4] + ".parquet";
  const std::string lineitem = data_path_ + "/" + kTableNames[0] + ".parquet";
  const std::string orders = data_path_ + "/" + kTableNames[1] + ".parquet";

  std::string sub_query = "select l_orderkey "
                          "from '" +
                          lineitem +
                          "' "
                          "group by l_orderkey "
                          "having sum(l_quantity) > 300";

  std::string query = "select c_name, c_custkey, o_orderkey, o_orderdate, "
                      "o_totalprice, sum(l_quantity) "
                      "from '" +
                      customer + "', '" + orders + "', '" + lineitem +
                      "' "
                      "where c_custkey = o_custkey "
                      "and l_orderkey = o_orderkey "
                      "and o_orderdate < '1995-03-15' "
                      "and l_orderkey in (" +
                      sub_query +
                      ") "
                      "group by c_name, c_custkey, o_orderkey, o_orderdate, "
                      "o_totalprice "
                      "order by o_totalprice desc, o_orderdate "
                      "limit 100";

  return ExecuteQueryInternal(query);
}

// PASSED
QueryStat QueryFactory::ExecuteQuery19() {
  const std::string lineitem = data_path_ + "/" + kTableNames[0] + ".parquet";
  const std::string part = data_path_ + "/" + kTableNames[2] + ".parquet";

  std::string query = "select sum(l_extendedprice * (1 - l_discount)) as "
                      "revenue "
                      "from '" +
                      lineitem + "', '" + part +
                      "' "
                      "where ( "
                      "p_partkey = l_partkey "
                      "and p_brand = 'Brand#12' "
                      "and p_container in ('SM CASE', 'SM BOX', 'SM PACK', "
                      "'SM PKG') "
                      "and l_quantity >= 1 and l_quantity <= 11 "
                      "and p_size between 1 and 5 "
                      "and l_shipmode in ('AIR', 'AIR REG') "
                      "and l_shipinstruct = 'DELIVER IN PERSON' "
                      ") or ( "
                      "p_partkey = l_partkey "
                      "and p_brand = 'Brand#23' "
                      "and p_container in ('MED BAG', 'MED BOX', 'MED PKG', "
                      "'MED PACK') "
                      "and l_quantity >= 10 and l_quantity <= 20 "
                      "and p_size between 1 and 10 "
                      "and l_shipmode in ('AIR', 'AIR REG') "
                      "and l_shipinstruct = 'DELIVER IN PERSON' "
                      ") or ( "
                      "p_partkey = l_partkey "
                      "and p_brand = 'Brand#34' "
                      "and p_container in ('LG CASE', 'LG BOX', 'LG PACK', "
                      "'LG PKG') "
                      "and l_quantity >= 20 and l_quantity <= 30 "
                      "and p_size between 1 and 15 "
                      "and l_shipmode in ('AIR', 'AIR REG') "
                      "and l_shipinstruct = 'DELIVER IN PERSON' "
                      ") GROUP BY NULL"; /* A trick to bypass "ungrouped" */

  return ExecuteQueryInternal(query);
}

// PASSED.
QueryStat QueryFactory::ExecuteQuery20() {
  const std::string part = data_path_ + "/" + kTableNames[2] + ".parquet";
  const std::string lineitem = data_path_ + "/" + kTableNames[0] + ".parquet";
  const std::string supplier = data_path_ + "/" + kTableNames[3] + ".parquet";
  const std::string nation = data_path_ + "/" + kTableNames[6] + ".parquet";
  const std::string partsupp = data_path_ + "/" + kTableNames[5] + ".parquet";

  std::string sub_query1 = "select p_partkey "
                           "from '" +
                           part +
                           "' "
                           "where p_name like 'forest%'";

  std::string sub_query2 = "select 0.5 * sum(l_quantity) "
                           "from '" +
                           lineitem +
                           "' "
                           "where l_partkey = ps_partkey "
                           "and l_suppkey = ps_suppkey "
                           "and l_shipdate >= '1994-01-01' "
                           "and l_shipdate < '1995-01-01'";

  std::string sub_query = "select ps_suppkey "
                          "from '" +
                          partsupp +
                          "' "
                          "where ps_partkey in (" +
                          sub_query1 +
                          ") "
                          "and ps_availqty > (" +
                          sub_query2 + ")";

  std::string query = "select s_name, s_address "
                      "from '" +
                      supplier + "', '" + nation +
                      "' "
                      "where s_suppkey in (" +
                      sub_query +
                      ") "
                      "and s_nationkey = n_nationkey "
                      "and n_name = 'CANADA' "
                      "order by s_name";

  return ExecuteQueryInternal(query);
}

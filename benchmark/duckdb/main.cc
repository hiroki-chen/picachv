#include <iostream>
#include <memory>

#include "cxxopts.h"
#include "duckdb.hpp"
#include "queries.h"

using namespace duckdb;

cxxopts::ParseResult ParseCommandLine(int argc, const char *argv[]) {
  std::unique_ptr<cxxopts::Options> allocated(
      new cxxopts::Options(argv[0], " - example command line options"));
  auto &options = *allocated;
  options.positional_help("[optional args]").show_positional_help();

  options.set_width(70)
      .set_tab_expansion()
      .allow_unrecognised_options()
      .add_options()("q,query-num", "The query number you want to execute",
                     cxxopts::value<int>())("h,help", "Print help")(
          "policy-path", "The path to the policy file",
          cxxopts::value<std::string>())("data-path",
                                         "The path to the data file",
                                         cxxopts::value<std::string>())(
          "enable-profiling", "Whether to enable profing on the Picachv side",
          cxxopts::value<bool>()->default_value("false"))(
          "t,thread-num", "The number of availble threads to use (0 = use all)",
          cxxopts::value<uint32_t>()->default_value("0"));

  return options.parse(argc, argv);
}

int main(int argc, const char *argv[]) {
  auto options = ParseCommandLine(argc, argv);

  // Set up the query factory.
  DuckDB db(nullptr);
  auto con = std::make_unique<duckdb::Connection>(db);
  std::unique_ptr<QueryFactory> factory =
      std::make_unique<QueryFactory>(options);
  if (!factory->Setup(std::move(con))) {
    std::cerr << "Failed to set up the query factory!" << std::endl;
    return 1;
  }

  // Execute the query.
  QueryStat stat = factory->ExecuteQuery();
  if (!stat.success) {
    std::cerr << "Query failed to execute!" << std::endl;
    return 1;
  }

  std::cout << "Query executed successfully! Time cost: " << stat.time.count()
            << " seconds." << std::endl;

  return 0;
}

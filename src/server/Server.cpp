#include <stdio.h>
#include <fstream>
#include <memory>
#include <thread>
#include <boost/program_options.hpp>

#include "Worker.h"
#include "QueryStrategy.h"
#include "ThreadStrategy.h"
#include "QueryStatistics.h"
#include "queries/Database.h"
#include "threads/Threads.h"
//#include "extrae_user_events.h"

namespace po = boost::program_options; // alias since its really verbose

int main(int argc, char *argv[]) {
  po::options_description description("Allowed options");
  description.add_options()("help", "produce help message")(
      "threads", po::value<unsigned int>()->default_value(1),
      "Number of threads")(
      "threadstrategy,t", po::value<std::string>()->default_value("roundrobin"),
      "The thread job distributation strategy generator [roundrobin]")(
      "database", po::value<std::string>()->default_value("snb.gdb"),
      "sparksee database path")("configuration", po::value<std::string>(),
                                "sparksee database path")(
      "port", po::value<unsigned int>()->default_value(9998),
      "Listening port of query petitions")(
      "shutdown", po::value<unsigned int>()->default_value(9999),
      "Listening port of shutdown petition")(
      "querystrategy,q", po::value<std::string>()->default_value("roundrobin"),
      "The query distributation strategy generator [roundrobin]")(
      "offline.json.file",
      po::value<std::string>()->default_value("params.json"),
      "The query distributation json parameters file")(
      "offline.number.queries", po::value<unsigned int>()->default_value(10),
      "number of queries to execute in an offline environment");

  po::variables_map vm;
  if (argc == 1) { // No arguments
    std::ifstream settings_file("settings.ini", std::ifstream::in);
    po::store(po::parse_config_file(settings_file, description), vm);
    po::notify(vm);
  } else {
    po::store(po::parse_command_line(argc, argv, description), vm);
    po::notify(vm);
  }

  if (vm.count("help") != 0) {
    std::cout << description << std::endl;
    return 1;
  }

  sparksee::Database database;
  try {
    if (vm.count("configuration") == 0) {
      database.open(vm["database"].as<std::string>().c_str());
    } else {
      database.open(vm["database"].as<std::string>().c_str(),
                    vm["configuration"].as<std::string>().c_str());
    }
  }
  catch (...) { // Sparksee does not throw std::exceptions or standard ones and
                // I do not want to include the header here destroying the proxy
                // class purpose.
    fprintf(stderr, "Unable to open the database\n");
    exit(1);
  }

  unsigned int port = vm["port"].as<unsigned int>();
  unsigned int shutdown_port = vm["shutdown"].as<unsigned int>();
  std::string query_str = vm["querystrategy"].as<std::string>();
  std::string thread_str = vm["threadstrategy"].as<std::string>();
  std::string json_file = vm["offline.json.file"].as<std::string>();
  unsigned int num_queries = vm["offline.number.queries"].as<unsigned int>();
  unsigned int num_threads = vm["threads"].as<unsigned int>();

  std::vector<ldbc_server::Worker*> workers;
  for( unsigned int i = 0; i < num_threads; ++i) {
	ldbc_server::Worker* worker = new ldbc_server::Worker(i,ldbc_server::Worker::eReader, &database);
      //workers.emplace_back(i, ldbc_server::Worker::eReader, &database);
	workers.push_back(worker);
  }
  std::vector<std::thread> threads;
  for (unsigned int i = 0; i < num_threads; ++i) {
    std::cout << "Created thread " << i << std::endl;
    threads.push_back(
        std::thread(ldbc_server::Worker::run, std::ref(*workers[i])));

  }
  //set_thread_affinity(threads);

  auto query_strategy = ldbc_server::QueryStrategy::Factory(
      &database, 
      query_str.c_str(), 
      json_file.c_str(), 
      num_queries, port,
      shutdown_port);

  auto thread_strategy =
      ldbc_server::ThreadStrategy::Factory(thread_str.c_str(), workers);


  timespec start, finish;
  clock_gettime(CLOCK_MONOTONIC, &start);
  std::thread query_reader([query_strategy, thread_strategy]() mutable {
      int i = 0;
      while (!query_strategy->end()) {
      ldbc_server::Worker::Task job = query_strategy->next();
      thread_strategy->next(job);
      i++;
      //if( i % 100 == 0 ) std::cout << "Served " << i << "queries" << std::endl;
      }
      });
  query_reader.detach();

  while(!query_strategy->end()){
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }


  std::cout << "Waiting for current executing threads to finish" << std::endl;

  for (unsigned int i = 0; i < num_threads; ++i) {
    workers[i]->end();
  }

  for (unsigned int i = 0; i < num_threads; ++i) {
    threads[i].join();
  }
  //query_reader.join();

  clock_gettime(CLOCK_MONOTONIC, &finish);
  float elapsed = (finish.tv_sec - start.tv_sec);
  elapsed += (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
  std::cout << "Execution Time " << elapsed << std::endl;

  ldbc_server::QueryStatistics statistics;
  for (unsigned int i = 0; i < num_threads; ++i) {
    std::cout << "Worker " << i << "executed " << workers[i]->stats()->total_count() << " queries" << std::endl;
    statistics.add(workers[i]->stats());
  }

  delete thread_strategy;
  delete query_strategy;
  database.shutdown();
  statistics.print();
  statistics.print_detail("detail.csv");
  return 0;
}

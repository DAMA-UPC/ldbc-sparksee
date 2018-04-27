
#include "gdb/Sparksee.h"
#include "gdb/Database.h"
#include "gdb/Session.h"
#include "gdb/Graph.h"
#include <sys/time.h>
#include <thread>
#include "server/ThreadStrategy.h"
//#include "queries/Database.h"
#include "utils/Utils.h"
#include "threads/Threads.h"

namespace gdb = sparksee::gdb;
namespace utils = sparksee::utils; 


int main(int argc, char **argv) {


  int num_threads = std::atoi(argv[1]);
  int num_opts = std::atoi(argv[2]);
  int opts_per_thread = num_opts / num_threads;

  gdb::SparkseeConfig cfg;
  gdb::Sparksee* sparksee = new gdb::Sparksee(cfg);

  gdb::Database *database = sparksee->Create(L"prova.gdb", L"prova");;
  gdb::Session* session = database->NewSession();
  gdb::Graph* graph = session->GetGraph();
  gdb::type_t nodeType = graph->NewNodeType(L"aux");
  gdb::attr_t attributeType = graph->NewAttribute(nodeType,L"auxAttribute", gdb::DataType::Integer, gdb::AttributeKind::Basic);
  gdb::oid_t node = graph->NewNode(nodeType);
  gdb::Value val;
  graph->SetAttribute(node, attributeType, val.SetInteger(12345));

  timespec start, finish;
  clock_gettime(CLOCK_MONOTONIC, &start);
  std::vector<std::thread> threads;
  for( int i = 0; i < num_threads; ++i ) {
    threads.push_back(std::thread( [&] () {
          gdb::Value val;
          utils::SessionPtr session(database->NewSession());
          utils::GraphPtr graph(session->GetGraph());
          session->Begin();
          for(int j = 0 ; j < opts_per_thread; ++j) {
          if( j % 10000 == 0 ) printf("Thread executed %d operations\n",j);
		sparksee::utils::ObjectsPtr obj(session->NewObjects());
          graph->GetAttribute(node, attributeType, val);
          }
          session->Commit();
          } ));
  }

  set_thread_affinity(threads);

  for( std::thread &t : threads) {
    t.join();
  }

  clock_gettime(CLOCK_MONOTONIC, &finish);
  float elapsed = (finish.tv_sec - start.tv_sec);
  elapsed += (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
  std::cout << "Execution Time before closing " << elapsed << std::endl;
  std::cout << "Transactions per second before closing " << num_opts/elapsed << std::endl;

  delete graph;
  delete session;
  delete database;

  clock_gettime(CLOCK_MONOTONIC, &finish);
  elapsed = (finish.tv_sec - start.tv_sec);
  elapsed += (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
  std::cout << "Execution Time " << elapsed << std::endl;
  std::cout << "Transactions per second " << num_opts/elapsed << std::endl;

  /*database = sparksee->Open(L"prova.gdb", false);
  delete database;
  */
  delete sparksee;
  return 0;
}

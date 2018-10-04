
#include "tools/snbLoader.h"
#include "gdb/Sparksee.h"
#include "gdb/Database.h"
#include "gdb/Session.h"
#include "gdb/Graph.h"
#include <sys/time.h>

namespace gdb = sparksee::gdb;

int main(int argc, char **argv) {

  gdb::SparkseeConfig cfg;
  gdb::Sparksee* sparksee = new gdb::Sparksee(cfg);
  std::string filename("./test.gdb");
  std::wstring wfilename(filename.begin(), filename.end());
  gdb::Database* database = sparksee->Create(wfilename, L"test");
  gdb::Session* session = database->NewSession();
  gdb::Graph* graph = session->GetGraph();
  gdb::type_t node_type = graph->NewNodeType(L"node_type");
  //gdb::attr_t attribute_type = graph->NewAttribute(node_type, L"attribute_type", gdb::Integer, gdb::Indexed);
  gdb::attr_t attribute_type = graph->NewAttribute(node_type, L"attribute_type", gdb::Integer, gdb::Basic);

  srand (time(NULL));

  int num_nodes = 100000000;
  int block_size = 50000;
  double average_tp = 0;

  try{
  std::cout << "Start load test " << std::endl;
  timeval start,end,start_block;
  gettimeofday(&start,NULL);
  start_block=start;
  for(int i = 0; i < num_nodes; ++i) {
    if((i+1) % block_size == 0) {
      gettimeofday(&end,NULL);
      long long elapsed_time = (end.tv_sec*1000 + end.tv_usec*0.001) - (start_block.tv_sec*1000.0 + start_block.tv_usec*0.001);
      double tp  = block_size / (elapsed_time/1000.0); 
      std::cout << i+1 << " Nodes loaded. tp: " << tp << " nodes/second" << std::endl;
      average_tp+=tp;
      gettimeofday(&start_block,NULL);
    }
    gdb::oid_t oid = graph->NewNode(node_type);
    graph->SetAttribute(oid, attribute_type, gdb::Value().SetInteger(rand()));
  }
  long long elapsed_time = (end.tv_sec*1000 + end.tv_usec*0.001) - (start.tv_sec*1000.0 + start.tv_usec*0.001);
  std::cout << "Finished test in " << elapsed_time << " ms. Average tp:" << average_tp << std::endl;
  }catch(gdb::Exception& e) {
    std::cout << e.Message() << std::endl;
  }

  delete graph;
  delete session;
  delete database;
  delete sparksee;

}

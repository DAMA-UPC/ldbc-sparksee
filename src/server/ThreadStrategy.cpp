#include "ThreadStrategy.h"
#include "ThreadStrategy/RoundRobin.h"
#include "ThreadStrategy/ShortestJobFirst.h"
#include "ThreadStrategy/UpdateBatching.h"
#include "ThreadStrategy/DoubleBuffer.h"
#include "ThreadStrategy/DoubleBufferEncore.h"
#include <string.h>
#include <iostream>

namespace ldbc_server {
ThreadStrategy *ThreadStrategy::Factory(const char *name, std::vector<Worker*> &workers) {
    
  ThreadStrategy* threadStrategy;
  if( strcmp(name,"roundrobin") == 0 ) {
     threadStrategy = new thread_strategy::RoundRobin(workers);
  } else if (strcmp(name,"shortestjobfirst") == 0) {
    threadStrategy = new thread_strategy::ShortestJobFirst(workers);
  } else if (strcmp(name, "updatebatching") == 0) {
    threadStrategy = new thread_strategy::UpdateBatching(workers);
  } else if (strcmp(name, "doublebuffer") == 0) {
    threadStrategy  = new thread_strategy::DoubleBuffer(workers);
  } else if (strcmp(name, "doublebufferencore") == 0) {
    threadStrategy =  new thread_strategy::DoubleBufferEncore(workers);
  }  else {
    std::cout << "WARNING: UNKNOWN THREAD STRATEGY " << name << " USING DEFAULT ROUND ROBIN" << std::endl;
    return new thread_strategy::RoundRobin(workers);
  }

  std::cout << "Executing with thread strategy " << name << std::endl;
  return threadStrategy;
}


}


#include "Threads.h"
void set_thread_affinity(std::vector<std::thread>& threads) {

  unsigned int num_nodes = 1;
  unsigned int num_cores_per_node = 2;
  unsigned int num_threads_per_core = 2;
  unsigned int num_cores = num_nodes*num_cores_per_node*num_threads_per_core;
  //unsigned int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
  //unsigned int num_physical_per_node = num_physical_cores / numa_nodes;
  
  unsigned int *cores = new unsigned int[num_cores];
  int index = 0;
  for(unsigned int i = 0; i < num_nodes; ++i) {
    for(unsigned int j = 0; j < num_cores_per_node; ++j ) {
      for(unsigned int k = 0; k < num_threads_per_core; ++k ) {
        cores[index++] = (i*num_cores_per_node)+(j*num_threads_per_core) + k;
      }
    }
  }


  for(unsigned int i = 0; i < threads.size(); i++) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);

    unsigned int core_id = cores[i%num_cores]; 
    CPU_SET(core_id, &cpuset);

    printf("thread %u assigned to core %u \n",i,core_id);
    pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
  }
  delete []cores;
}

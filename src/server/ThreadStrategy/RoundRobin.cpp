#include "RoundRobin.h"
#include "server/Worker.h"
#include <iostream>



namespace ldbc_server {
namespace thread_strategy {
void RoundRobin::next(Worker::Task job) {
  if (workers_.size() == 0) {
    return;
  }
  workers_[(next_++) % workers_.size()]->add_job(job);
}
}
}

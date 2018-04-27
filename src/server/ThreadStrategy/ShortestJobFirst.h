#ifndef _THREAD_STRATEGY_SHORTEST_JOB_FIRST_H_
#define _THREAD_STRATEGY_SHORTEST_JOB_FIRST_H_

#include "server/ThreadStrategy.h"
#include <vector>

namespace ldbc_server {
class Worker;

namespace thread_strategy {
class ShortestJobFirst : public ThreadStrategy {
public:
  ShortestJobFirst(std::vector<Worker*> &workers) : workers_(workers) {}
  ~ShortestJobFirst() {}
  virtual void next(Worker::Task job) override;

private:
  std::vector<Worker*> &workers_;
};
}
}

#endif /* _THREAD_STRATEGY_SHORTEST_JOB_FIRST_H_ */

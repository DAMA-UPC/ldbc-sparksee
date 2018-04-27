#ifndef _THREAD_STRATEGY_ROUND_ROBIN_H_
#define _THREAD_STRATEGY_ROUND_ROBIN_H_

#include "server/ThreadStrategy.h"
#include <vector>

namespace ldbc_server {
class Worker;

namespace thread_strategy {
class RoundRobin : public ThreadStrategy {
public:
  RoundRobin(std::vector<Worker*> &workers) : next_(0), workers_(workers) {}
  ~RoundRobin() {}
  virtual void next(Worker::Task job) override;

private:
  size_t next_;
  std::vector<Worker*> &workers_;
};
}
}

#endif /* _THREAD_STRATEGY_ROUND_ROBIN_H_ */

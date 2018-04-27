#ifndef _THREAD_STRATEGY_H_
#define _THREAD_STRATEGY_H_

#include "server/Worker.h"
#include <vector>
#include <thread>
#include <pthread.h>

namespace ldbc_server {


class Worker;
class ThreadStrategy {
public:
  static ThreadStrategy *Factory(const char *name, std::vector<Worker*> &workers);
  virtual ~ThreadStrategy() {}

  virtual void next(Worker::Task job) = 0;
};
}

#endif /* _THREAD_STRATEGY_H_ */

#ifndef _THREAD_STRATEGY_DOUBLE_BUFFER_ENCORE_H_
#define _THREAD_STRATEGY_DOUBLE_BUFFER_ENCORE_H_

#include "server/ThreadStrategy.h"
#include <vector>
#include <mutex>
#include <thread>

namespace ldbc_server {
class Worker;

namespace thread_strategy {
class DoubleBufferEncore : public ThreadStrategy {
public:
  DoubleBufferEncore(std::vector<Worker*> &workers);
  ~DoubleBufferEncore();
  virtual void next(Worker::Task job) override;

  bool running() {return is_running_;}
  void switch_mode();

  static void polling(DoubleBufferEncore *strategy, unsigned long time);

private:
  Worker* find_free_worker();
  void query_ended(int worker_id, int query_id, long unsigned time);

  bool is_running_;
  std::mutex mutex_; 
  unsigned long switch_time_;
  std::atomic<bool> is_updating_;
  std::vector<Worker::Task> query_queue_;
  std::vector<Worker::Task> update_queue_;
  std::vector<Worker::Task> slow_queue_;
  std::vector<Worker*> &workers_;
  std::thread thread_;
};
}
}

#endif /* _THREAD_STRATEGY_DOUBLE_BUFFER_ENCORE_H_ */

#ifndef _THREAD_STRATEGY_DOUBLE_BUFFER_H_
#define _THREAD_STRATEGY_DOUBLE_BUFFER_H_

#include "server/ThreadStrategy.h"
#include <vector>
#include <mutex>
#include <thread>

namespace ldbc_server {
class Worker;

namespace thread_strategy {
class DoubleBuffer : public ThreadStrategy {
public:
  DoubleBuffer(std::vector<Worker*> &workers);
  ~DoubleBuffer();
  virtual void next(Worker::Task job) override;

  bool running() {return is_running_;}
  void switch_mode();

  static void polling(DoubleBuffer *strategy, unsigned long time);

private:
  void add_job_to_worker(std::vector<Worker*> &workers, Worker::Task job);
  void query_ended(int worker_id, int query_id, long unsigned time);

  bool is_running_;
  std::atomic<int> dispatched_updates_;  
  std::mutex mutex_; 
  unsigned long timer_;
  unsigned long switch_time_;
  std::atomic<bool> is_updating_;
  std::vector<Worker::Task> query_queue_;
  std::vector<Worker::Task> update_queue_;
  std::vector<Worker*> &workers_;
  std::thread thread_;
};
}
}

#endif /* _THREAD_STRATEGY_DOUBLE_BUFFER_H_ */

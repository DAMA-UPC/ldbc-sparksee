#ifndef _THREAD_STRATEGY_UPDATE_BATCHING_H_
#define _THREAD_STRATEGY_UPDATE_BATCHING_H_

#include "server/ThreadStrategy.h"
#include "server/Worker.h"
#include <vector>
#include <mutex>
#include <atomic>

namespace ldbc_server {

namespace thread_strategy {
class UpdateBatching : public ThreadStrategy {
public:
  UpdateBatching(std::vector<Worker*> &workers);
  ~UpdateBatching() {}
  virtual void next(Worker::Task job) override;

  void query_started(int worker_id, int query_id);
  void query_ended(int worker_id, int query_id, long unsigned time);

private:
  void add_job_to_worker(std::vector<Worker*> &workers, Worker::Task job);

  std::vector<Worker*> &workers_;
  std::mutex mutex_;
  std::atomic<unsigned long> total_time_;
  std::atomic<unsigned long> total_count_;
  float slow_query_threshold_;
  std::atomic<unsigned long> slow_query_time_;
  std::atomic<unsigned long> slow_queries_ongoing_;
  std::atomic<unsigned long> updates_enqueued_;
  std::vector<unsigned long> query_count_;
  std::vector<unsigned long> time_;
  std::vector<int> on_going_queries_;
  std::vector<unsigned long> slow_times_;
  std::vector<unsigned long> query_old_times_;
  std::vector<std::vector<Worker::Task>> pending_queries_;
};
}
}

#endif /* _THREAD_STRATEGY_UPDATE_BATCHING_H_ */

#include "UpdateBatching.h"
#include "server/Worker.h"
#include "server/QueryStatistics.h"
#include <iostream>
#include <algorithm>



namespace ldbc_server {
namespace thread_strategy {

UpdateBatching::UpdateBatching(std::vector<Worker*> &workers)
    : workers_(workers)
    , total_time_(0), total_count_(0) , slow_query_threshold_(2.0f)
    , slow_query_time_(std::numeric_limits<unsigned long>::max()) 
    , slow_queries_ongoing_(0)
    , updates_enqueued_(0)  {
  query_count_.resize(QueryId::kNumQueries, 0);
  time_.resize(QueryId::kNumQueries, 0);
  on_going_queries_.resize(QueryId::kNumQueries, 0);
  pending_queries_.resize(2);

  query_old_times_.resize(workers.size(), 0);
  slow_times_.resize(workers.size(), std::numeric_limits<unsigned long>::max());
  for (auto worker : workers) {
    worker->subscribe_on_start(std::bind(
        [this](int id, int query_id) { this->query_started(id, query_id); }, std::placeholders::_1, std::placeholders::_2));
    worker->subscribe_on_end(std::bind([this](int id, int query_id, unsigned long time) {
      this->query_ended(id, query_id, time);
      }, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
  }
}

void UpdateBatching::add_job_to_worker(std::vector<Worker*> &workers, Worker::Task job) {
  /*auto worker = std::min_element(workers.begin(), workers.end() - 1,
                      [](Worker &worker_a, Worker &worker_b) {
                         return worker_a.num_jobs() < worker_b.num_jobs();
                       });
  worker->add_job(job);*/
}

void UpdateBatching::next(Worker::Task job) {
  if (workers_.size() == 0) {
    return;
  } else if (workers_.size() == 1) {
    workers_[0]->add_job(job);
    return;
  }
  mutex_.lock();
  size_t query_id = std::get<0>(job);
  bool i_am_slow = false;
  if (query_count_[query_id] > 0 && (query_id < update_range::kMin || query_id > update_range::kMax)) {
    i_am_slow = (time_[query_id] / query_count_[query_id] >= slow_query_time_);
  }
  
  if (i_am_slow && updates_enqueued_ > 0) {
    pending_queries_[0].push_back(job);
    mutex_.unlock();
    return;
  }

  if (slow_queries_ongoing_ > 0) {

    if (query_id >= update_range::kMin && query_id <= update_range::kMax) {
      pending_queries_[1].push_back(job);
      mutex_.unlock();
    } else if (i_am_slow) {
      pending_queries_[0].push_back(job);
      mutex_.unlock();
    } else {
      mutex_.unlock();
      add_job_to_worker(workers_, job);
    }
  } else {
    mutex_.unlock();
    add_job_to_worker(workers_, job);
  }
}

void UpdateBatching::query_started(int worker_id, int query_id) {
  mutex_.lock();
  size_t query_idd = static_cast<size_t>(query_id);
  on_going_queries_[query_idd]++;

  if (query_count_[query_idd] > 0 && (time_[query_idd] / query_count_[query_idd] >= slow_query_time_) ) {
    slow_queries_ongoing_++;
  }
  
  query_old_times_[static_cast<size_t>(worker_id)] = (query_count_[query_idd] == 0) ? 0 : time_[query_idd] / query_count_[query_idd];
  slow_times_[static_cast<size_t>(worker_id)] = slow_query_time_;

  mutex_.unlock();
}

void UpdateBatching::query_ended(int worker_id, int query_id, long unsigned time)
{
  mutex_.lock();
  size_t query_idd = static_cast<size_t>(query_id);
  unsigned long query_time = query_old_times_[static_cast<size_t>(worker_id)];
  if (updates_enqueued_ > 0 && query_id >= update_range::kMin && query_id <= update_range::kMax) {
    --updates_enqueued_;
  }
  if (slow_queries_ongoing_ > 0 && (query_time >= slow_times_[static_cast<size_t>(worker_id)]) ) {
    slow_queries_ongoing_--;
  }
 
  on_going_queries_[query_idd]--;
  query_count_[query_idd]++;
  time_[query_idd] += time;
  total_time_ += time; 
  total_count_++;

  slow_query_time_ = static_cast<unsigned long>(slow_query_threshold_ * 
                                  (static_cast<float>(total_time_) / static_cast<float>(total_count_)));
  if (slow_queries_ongoing_ == 0) {
    if (pending_queries_[1].size() > 0) {
      std::vector<Worker::Task> copied = pending_queries_[1];
      pending_queries_[1].clear();
      updates_enqueued_ = pending_queries_[1].size();
      mutex_.unlock();
      for (auto &job : copied) {
        workers_[workers_.size() - 1]->add_job(job);
      }
    } else if (updates_enqueued_ == 0 && pending_queries_[0].size() > 0) {
      auto job = pending_queries_[0].front();
      pending_queries_[0].erase(pending_queries_[0].begin());
      mutex_.unlock();
      add_job_to_worker(workers_, job);
    } else {
      mutex_.unlock();
    }
  } else {
    mutex_.unlock();
  }
}
}
}

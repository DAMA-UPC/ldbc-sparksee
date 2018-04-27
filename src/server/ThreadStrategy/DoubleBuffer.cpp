#include "DoubleBuffer.h"
#include "server/Worker.h"
#include "server/utils/Timestamps.h"
#include <iostream>
#include <algorithm>
#include <thread>
#include <chrono>


namespace ldbc_server {
namespace thread_strategy {

DoubleBuffer::DoubleBuffer(std::vector<Worker*> &workers)
    : is_running_(true)
    , timer_(timestamp::current())
    , switch_time_(1000)
    , is_updating_(false)
    , workers_(workers)
    , thread_(DoubleBuffer::polling, this, switch_time_)
{ 
  for (auto worker : workers) {
    worker->subscribe_on_end(std::bind([this](int id, int query_id, unsigned long time) {
      this->query_ended(id, query_id, time);
      }, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
  }
}

DoubleBuffer::~DoubleBuffer() {
  is_running_ = false;
  thread_.join();
}

void DoubleBuffer::polling(DoubleBuffer *strategy, unsigned long timer)
{
  while (strategy->running()) {
    if (!strategy->running()) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(timer));
    strategy->switch_mode();
  }
} 

void DoubleBuffer::switch_mode() {
  is_updating_ = !is_updating_;
  if (is_updating_) {
    mutex_.lock();
    for (auto &job : update_queue_) {
      workers_[workers_.size() - 1]->add_job(job);
    }
    update_queue_.clear();
    mutex_.unlock();
  } else {
    mutex_.lock();
    for (auto &job : query_queue_) {
      add_job_to_worker(workers_, job);
    }
    query_queue_.clear();
    mutex_.unlock();
  }
}

void DoubleBuffer::add_job_to_worker(std::vector<Worker*> &workers, Worker::Task job) {
  /*auto worker = std::min_element(workers.begin(), workers.end() - 1,
                      [](Worker &worker_a, Worker &worker_b) {
                         return worker_a.num_jobs() < worker_b.num_jobs();
                       });
  worker->add_job(job);
*/
}

void DoubleBuffer::next(Worker::Task job) {
  if (workers_.size() == 0) {
    return;
  }
  int query_id = std::get<0>(job);

  if (is_updating_) {
    if (query_id >= update_range::kMin && query_id <= update_range::kMax) {
      ++dispatched_updates_;
      workers_[workers_.size() - 1]->add_job(job);
    } else {
      mutex_.lock();
      query_queue_.push_back(job);
      mutex_.unlock();
    }
  } else {
    if (query_id >= update_range::kMin && query_id <= update_range::kMax) { 
      mutex_.lock();
      update_queue_.push_back(job);
      mutex_.unlock();
    } else {
      add_job_to_worker(workers_, job);
    }
  }
}

void DoubleBuffer::query_ended(int, int query_id, long unsigned)
{
  if (query_id >= update_range::kMin && query_id <= update_range::kMax) {
    --dispatched_updates_;
  }
}

}
}

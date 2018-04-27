#include "DoubleBufferEncore.h"
#include "server/Worker.h"
#include "server/utils/Timestamps.h"
#include <iostream>
#include <algorithm>
#include <thread>
#include <chrono>


namespace ldbc_server {
namespace thread_strategy {

DoubleBufferEncore::DoubleBufferEncore(std::vector<Worker*> &workers)
    : is_running_(true)
    , switch_time_(250)
    , is_updating_(false)
    , workers_(workers)
    , thread_(DoubleBufferEncore::polling, this, switch_time_)
{ 
  for (auto worker : workers) {
    worker->subscribe_on_end(std::bind([this](int id, int query_id, unsigned long time) {
      this->query_ended(id, query_id, time);
      }, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
  }
}

DoubleBufferEncore::~DoubleBufferEncore() {
  is_running_ = false;
  thread_.join();
}

void DoubleBufferEncore::polling(DoubleBufferEncore *strategy, unsigned long timer)
{
  while (strategy->running()) {
    if (!strategy->running()) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(timer));
    strategy->switch_mode();
  }
} 

void DoubleBufferEncore::switch_mode() {
  is_updating_ = !is_updating_;
  mutex_.lock();
  if ((is_updating_ && update_queue_.size() == 0) ||
      (!is_updating_ && slow_queue_.size() == 0 && query_queue_.size() == 0)) {
    is_updating_ = !is_updating_;
  }

  if (is_updating_) {
    for (auto worker = find_free_worker(); update_queue_.size() > 0 && worker != nullptr; worker = find_free_worker()) {
      auto old_job = update_queue_.front();
      update_queue_.erase(update_queue_.begin()); 
      mutex_.unlock();
      worker->add_job(old_job);
      mutex_.lock();
    }
  } else {
    if (slow_queue_.size() > 0) {
      query_queue_.insert(query_queue_.begin(), slow_queue_.begin(), slow_queue_.end());
      slow_queue_.clear();
    }
    for (auto worker = find_free_worker(); query_queue_.size() > 0 && worker != nullptr; worker = find_free_worker()) {
      auto old_job = query_queue_.front();
      query_queue_.erase(query_queue_.begin()); 
      mutex_.unlock();
      worker->add_job(old_job);
      mutex_.lock();
    }
  }
  mutex_.unlock();
}

Worker* DoubleBufferEncore::find_free_worker() {
  auto pointer = std::find_if(workers_.begin(), workers_.end(),
                      [](Worker* worker_a) {
                         return worker_a->is_unemployed();
                       });
  if (pointer == workers_.end()) {
    return nullptr;
  }
  return (*pointer);
}

void DoubleBufferEncore::next(Worker::Task job) {
  if (workers_.size() == 0) {
    return;
  }

  int query_id = std::get<0>(job);

  mutex_.lock();
  if (query_id >= update_range::kMin && query_id <= update_range::kMax) {
    update_queue_.push_back(job);
  } else if (query_id == kInteractive9 || query_id == kInteractive6) {
    slow_queue_.push_back(job);
  } else {
    query_queue_.push_back(job);
  }

  auto worker = find_free_worker();
  if (worker != nullptr) {
    if (is_updating_) {
      if (update_queue_.size() > 0) {
        auto old_job = update_queue_.front();
        update_queue_.erase(update_queue_.begin()); 
        mutex_.unlock();
        worker->add_job(old_job);
      } else {
        mutex_.unlock();
      }
    } else {
      if (query_queue_.size() > 0) {
        auto old_job = query_queue_.front();
        query_queue_.erase(query_queue_.begin());
        mutex_.unlock();
        worker->add_job(old_job);
      } else {
        mutex_.unlock();
      }
    }
  } else {
    mutex_.unlock();
  }
  for (auto slave : workers_) {
    slave->notify();
  }
}

void DoubleBufferEncore::query_ended(int worker_id, int, long unsigned) {
  mutex_.lock();
  if (is_updating_) {
    if (update_queue_.size() > 0) {
      auto old_job = update_queue_.front();
      update_queue_.erase(update_queue_.begin()); 
      mutex_.unlock();
      workers_[static_cast<size_t>(worker_id)]->add_job(old_job);
    } else {
      mutex_.unlock();
    }
  } else {
    if (query_queue_.size() > 0) {
      auto old_job = query_queue_.front();
      query_queue_.erase(query_queue_.begin());
      mutex_.unlock();
      workers_[static_cast<size_t>(worker_id)]->add_job(old_job);
    } else {
      mutex_.unlock();
    }
  }
  
  for (auto &slave : workers_) {
    slave->notify();
  }
}

}
}

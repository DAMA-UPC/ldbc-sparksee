#include "Worker.h"
#include "QueryStatistics.h"
#include "utils/Timestamps.h"
#include "utils/DataTypes.h"
#include <iostream>

namespace ldbc_server {

Worker::Worker( int id, WorkerType type, sparksee::Database* database ) : id_(id), end_(false), jobs_(1024), database_(database) {
}

/*Worker::Worker(const Worker &worker)  { 
    id_ = worker.id_;
    end_ = worker.end_.load();
    session_ = worker.session_;
}

Worker::Worker(const Worker &&worker) {
    id_ = worker.id_;
    end_ = worker.end_.load();
}
*/

Worker::~Worker() { }

void Worker::end() {
  end_ = true;
  condition_.notify_all();
}

void Worker::wait() {
  std::unique_lock<std::mutex> cond_mutex(mutex_);
  condition_.wait(cond_mutex, [this] {
    return this->has_ended() || !this->is_unemployed();
  });
  //cond_mutex.unlock();
}

void Worker::add_job(Task job) {
 /* job_mutex_.lock();
  jobs_.push_back(job);
  job_mutex_.unlock();
*/
  std::unique_lock<std::mutex> cond_mutex(mutex_);
	jobs_.push(job);
//  std::cout << "Query " << std::get<0>(job) << " assigned to worker " << id_ << std::endl; 

  condition_.notify_one();
  //cond_mutex.unlock();
}

  /*int Worker::num_jobs() {
      job_mutex_.lock();
      unsigned long size = jobs_.size();
      job_mutex_.unlock();
      return static_cast<int>(size);
  }*/

bool Worker::is_unemployed() {
  //return num_jobs() == 0;
	return jobs_.empty();
}

void Worker::execute_job() {
	/*job_mutex_.lock();
	if (jobs_.size() > 0) {
		Task job = jobs_[0];
		job_mutex_.unlock();
		unsigned long pre_execution = timestamp::current_micros();
		on_start_query(std::get<0>(job));
		datatypes::Buffer buffer = std::get<2>(job)(session_);
		unsigned long post_execution = timestamp::current_micros();
		on_end_query(std::get<0>(job), post_execution - pre_execution);
		unsigned long dispatched = std::get<1>(job);
		std::get<3>(job)(buffer);
		buffer.free();
		unsigned long all_time = timestamp::current_micros();
		std::get<4>(job)();

		stats_.add(std::get<0>(job), all_time - dispatched,
				dispatched, pre_execution);
		job_mutex_.lock();
		jobs_.erase(jobs_.begin());
	}
	job_mutex_.unlock();
*/

	if (!jobs_.empty()) {
		Task job;
 		jobs_.pop(job);
    //std::cout << "Worker " << id_ << " picked query " << std::get<0>(job) << std::endl; 
		unsigned long pre_execution = timestamp::current_micros();
		on_start_query(std::get<0>(job));
		datatypes::Buffer buffer = std::get<2>(job)(session_);
		unsigned long post_execution = timestamp::current_micros();
		on_end_query(std::get<0>(job), post_execution - pre_execution);
		unsigned long dispatched = std::get<1>(job);
		std::get<3>(job)(buffer);
		buffer.free();
		unsigned long all_time = timestamp::current_micros();
		std::get<4>(job)();

		stats_.add(std::get<0>(job), all_time - dispatched,
				dispatched, pre_execution);
	}
}

void Worker::run(Worker &worker) {
  worker.session_ = worker.database_->new_session();
	while (!worker.has_ended() || !worker.is_unemployed()) {
		worker.wait();
		while(!worker.is_unemployed()) {
			worker.execute_job();
		}
	}
  delete worker.session_;
}

void Worker::on_start_query(int query_id) {
  start_mutex_.lock();
  for (auto &action : start_consumers_) {
    action(id_, query_id);
  }
  start_mutex_.unlock();
}

void Worker::on_end_query(int query_id, unsigned long time) {
  end_mutex_.lock();
  for (auto &action : end_consumers_) {
    action(id_, query_id, time);
  }
  end_mutex_.unlock();
}

void Worker::subscribe_on_start(std::function<void(int, int)> action) 
{
  start_mutex_.lock();
  start_consumers_.push_back(action);
  start_mutex_.unlock();
}

void Worker::subscribe_on_end(std::function<void(int, int, unsigned long)> action) {
  end_mutex_.lock();
  end_consumers_.push_back(action);
  end_mutex_.unlock();
}

void Worker::notify() {
 condition_.notify_one(); 
}
}

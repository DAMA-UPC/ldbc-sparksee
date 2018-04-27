#ifndef _WORKER_H_
#define _WORKER_H_

#include <vector>
#include <functional>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <gdb/Session.h>
#include <queries/Database.h>
#include <boost/lockfree/spsc_queue.hpp>

#include "server/QueryStatistics.h"
#include "utils/DataTypes.h"

namespace gdb = sparksee::gdb;

namespace ldbc_server {
class QueryStatistics;
class Worker {
public:
  enum WorkerType {
    eReader,
    eWriter
  };

  // tuple representing a worker task it contains:
  // QueryId: Query Id enum @see QueryStatstics.h
  // unsigned long: time which the task was enqueued
  // std::function<const char*()>: the ldbc query function to execute.
  // std::function<void(std::string &>: the post process with the query result, for exemple, 
  //                                    in case of acting as a server send it to the client.
  // std::function<void>(): Clean up function, some queries requires dynamic memory (at least in
  //                        the current implementation) and this will release those resources.
  typedef std::tuple<QueryId, unsigned long, std::function< const datatypes::Buffer (gdb::Session* sess)>,
                     std::function<void(const datatypes::Buffer &)>, std::function<void()> > Task;

  explicit Worker( int id, WorkerType type, sparksee::Database* database ); 
  Worker(const Worker &worker);
  Worker(const Worker &&worker);
  virtual ~Worker();

  int id() const { return id_; }
  void wait();
  void add_job(Task job);
  //int num_jobs() ;
  void execute_job();
  bool is_unemployed();
  bool has_ended() { return end_; }
  void end();
  const QueryStatistics *stats() { return &stats_; }

  void notify();
  static void run(Worker &worker);
  void on_start_query(int query_id);
  void on_end_query(int query_id, unsigned long time);
  void subscribe_on_start(std::function<void(int, int)> action);
  void subscribe_on_end(std::function<void(int, int, unsigned long)> action);


private:
  int id_;
//  WorkerType type_;
  QueryStatistics stats_;
  std::atomic<bool> end_;
  std::mutex mutex_;
  std::mutex job_mutex_;
  std::condition_variable condition_;
  //std::vector<Task> jobs_;
	boost::lockfree::spsc_queue<Task> jobs_;
  std::mutex start_mutex_;
  std::mutex end_mutex_;
  std::vector<std::function<void(int, int)>> start_consumers_;
  std::vector<std::function<void(int, int, unsigned long)>> end_consumers_;

public:
  gdb::Session* session_;
  sparksee::Database* database_;
  
};
}

#endif /* _WORKER_H_ */

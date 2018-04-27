#ifndef _QUERY_STRATEGY_ROUND_ROBIN_H_
#define _QUERY_STRATEGY_ROUND_ROBIN_H_

#include "server/QueryStrategy.h"
#include "server/QueryStatistics.h"

namespace sparksee {
class Database;
}

namespace ldbc_server {
class Worker;

namespace query_strategy {
class RoundRobin : public QueryStrategy {
public:
  RoundRobin(sparksee::Database *database, const char *json_file,
             const unsigned int num_queries);
  ~RoundRobin() {}
  virtual Worker::Task next() override;

private:
  std::map<std::string, std::vector<std::string>> next_parameter(int id) override;
  size_t next_;
  unsigned int job_count_;
  unsigned int max_jobs_;
  std::vector<size_t> next_parameter_;
  std::vector<size_t> queries_with_parameters;
  size_t current_query_;
};
}
}

#endif /* _QUERY_STRATEGY_H_ */

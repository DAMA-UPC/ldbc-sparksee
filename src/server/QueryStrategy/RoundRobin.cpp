#include "RoundRobin.h"
#include "server/Worker.h"

#include "server/QueryStatistics.h"

namespace ldbc_server {
namespace query_strategy {
RoundRobin::RoundRobin(sparksee::Database *database, const char *json_file,
                       const unsigned int num_queries)
    : next_(0), job_count_(0), max_jobs_(num_queries) {
  database_ = database;
  end_ = job_count_ >= num_queries;
  populate_parameter_list(read_json(json_file));
  next_parameter_.resize(kNumQueries, 0);
  current_query_= 0;
  for( size_t i = 0; i < QueryId::kNumQueries; ++i) {
      if( parameters_[i].size() > 0UL){
          queries_with_parameters.push_back(i);
      }
  }
  next_ = queries_with_parameters[0];
};

std::map<std::string, std::vector<std::string>> RoundRobin::next_parameter(int id) {
  size_t idd = static_cast<size_t>(id);
  size_t pos = next_parameter_[idd];
  next_parameter_[idd] = (next_parameter_[idd] + 1UL) % parameters_[idd].size();
  return parameters_[idd][pos];
}

Worker::Task RoundRobin::next() {
  next_ = queries_with_parameters[current_query_];
  std::map<std::string, std::vector<std::string>> parameters = next_parameter(static_cast<int>(next_));
  current_query_ = (current_query_ + 1) % queries_with_parameters.size();
  end_ = (++job_count_) >= max_jobs_;
  return function_factory(parameters, std::bind([]() {}));
}
}
}

#ifndef _QUERY_STRATEGY_H_
#define _QUERY_STRATEGY_H_

#include <vector>
#include <map>
#include <functional>
#include <mutex>
#include <boost/property_tree/ptree.hpp>

#include "server/Worker.h"
#include "queries/snbInteractive.h"
#include "queries/snbBi.h"
#include "server/utils/DataTypes.h"

namespace sparksee {
class Database;
}

namespace ldbc_server {
class Worker;

class QueryStrategy {
public:
  static QueryStrategy *Factory(sparksee::Database *database, const char *name,
                                const char *json_file, unsigned int num_queries,
                                unsigned int port, unsigned int shutdown_port);
  virtual ~QueryStrategy() {}
  bool end() { return end_; }
  virtual Worker::Task next() = 0;

protected:
  const char* copy_string(std::map<std::string, std::vector<std::string>> &parameters, std::string name, size_t index);
  unsigned int parse_query_id(const std::string &element);
  void search_values(boost::property_tree::ptree tree, std::vector<std::string> &param_values);
  boost::property_tree::ptree read_json(const char *json_file);
  boost::property_tree::ptree read_json(std::stringstream &stream);
  void populate_parameter_list(boost::property_tree::ptree tree);
  unsigned int get_first_id(boost::property_tree::ptree tree);
  Worker::Task function_factory(std::map<std::string, std::vector<std::string>> parameters,
                                std::function<void(const datatypes::Buffer &)> callback);
  virtual std::map<std::string, std::vector<std::string>> next_parameter(int id) = 0;

  bool end_;
  sparksee::Database *database_;
  std::vector<std::vector<std::map<std::string, std::vector<std::string>> > > parameters_;
  std::mutex parameters_mutex_;
};
}

#endif /* _QUERY_STRATEGY_H_ */

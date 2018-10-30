#ifndef _QUERY_STRATEGY_REMOTE_H_
#define _QUERY_STRATEGY_REMOTE_H_

#include <boost/asio.hpp>

#include "server/QueryStrategy.h"
#include "server/QueryStatistics.h"

namespace sparksee {
class Database;
}

namespace ldbc_server {
class Worker;

namespace query_strategy {
class Remote : public QueryStrategy {
public:
  Remote(sparksee::Database *database, unsigned int port,
         unsigned int shutdown_port);
  ~Remote() {}
  virtual Worker::Task next() override;

private:
  void shutdown_wait();
  void server_start_conn();

  std::map<std::string, std::vector<std::string>> next_parameter(int id) override;
  boost::asio::io_service io_service_;
  boost::asio::ip::tcp::acceptor acceptor_;
  boost::asio::ip::tcp::acceptor shutdown_;
  boost::asio::ip::tcp::acceptor serverup_;
};
}
}

#endif /* _QUERY_STRATEGY_REMOTE_H_ */

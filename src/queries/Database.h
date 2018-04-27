#ifndef _DATABASE_H_
#define _DATABASE_H_

#include "boost/thread/mutex.hpp"

namespace sparksee {
namespace gdb {
class Sparksee;
class Database;
class Session;
}
class Database {
public:
  Database() : sparksee_(0), database_(0) {}
  ~Database() {}

  void open(const char *filename);
  void open(const char *filename, const char *configuration);
  void create(const char *filename);
  void shutdown();

  gdb::Session *new_session();

private:
  boost::mutex mutex_;
  gdb::Sparksee *sparksee_;
  gdb::Database *database_;
};
}

#endif /* _DATABASE_H_ */

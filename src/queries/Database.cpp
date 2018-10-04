#include "Database.h"

#include "boost/thread/mutex.hpp"

#include "gdb/Sparksee.h"
#include "gdb/Database.h"
#include <sys/time.h>

namespace sparksee {
void Database::open(const char *filename) {
  gdb::SparkseeConfig cfg;
  sparksee_ = new gdb::Sparksee(cfg);
  std::string tmp(filename);
  std::wstring wfilename(tmp.begin(), tmp.end());
  timeval start,end;
  std::cout << "Opening database " << tmp << " ... " << std::endl;
  gettimeofday(&start,NULL);
  database_ = sparksee_->Open(wfilename, false);
  gettimeofday(&end,NULL);
  long long elapsed_time = (end.tv_sec*1000 + end.tv_usec*0.001) - (start.tv_sec*1000.0 + start.tv_usec*0.001);
  std::cout << "Database opened in " << elapsed_time << " ms " << std::endl;
}
void Database::open(const char *filename, const char *configuration) {
  std::string tmp(configuration);
  std::wstring wconfiguration(tmp.begin(), tmp.end());
  gdb::SparkseeProperties::Load(wconfiguration);
  open(filename);
}

void Database::create(const char *filename) {
  gdb::SparkseeConfig cfg;
  sparksee_ = new gdb::Sparksee(cfg);
  std::string tmp(filename);
  std::wstring wfilename(tmp.begin(), tmp.end());
  timeval start,end;
  std::cout << "Creating database " << tmp << " ... " << std::endl;
  gettimeofday(&start,NULL);
  database_ = sparksee_->Create(wfilename, L"prova");
  gettimeofday(&end,NULL);
  long long elapsed_time = (end.tv_sec*1000 + end.tv_usec*0.001) - (start.tv_sec*1000.0 + start.tv_usec*0.001);
  std::cout << "Database created in " << elapsed_time << " ms " << std::endl;
}

gdb::Session *Database::new_session() {
  gdb::Session *session = 0;
  mutex_.lock();
  if (database_ != 0) {
    session = database_->NewSession();
  }
  mutex_.unlock();
  return session;
}

void Database::shutdown() {
  std::cout << "Shurting down Sparksee database" << std::endl;
  if (database_ != 0) {
      try{
    delete database_;
      } catch( gdb::Exception& e ) {
          std::cout << e.Message() << std::endl;
      }
  }
  if (sparksee_ != 0) {
    delete sparksee_;
  }
  database_ = NULL;
  sparksee_ = NULL;
}
}

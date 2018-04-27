#ifndef _QUERY_STATISTICS_H_
#define _QUERY_STATISTICS_H_

#include <vector>
#include <limits>

namespace ldbc_server {

enum QueryId {
  kInteractive1 = 0,
  kInteractive2,
  kInteractive3,
  kInteractive4,
  kInteractive5,
  kInteractive6,
  kInteractive7,
  kInteractive8,
  kInteractive9,
  kInteractive10,
  kInteractive11,
  kInteractive12,
  kInteractive13,
  kInteractive14,
  kUpdate1,
  kUpdate2,
  kUpdate3,
  kUpdate4,
  kUpdate5,
  kUpdate6,
  kUpdate7,
  kUpdate8,
  kShort1,
  kShort2,
  kShort3,
  kShort4,
  kShort5,
  kShort6,
  kShort7,
  kBi1,
  kBi2,
  kBi3,
  kBi4,
  kBi5,
  kBi6,
  kBi7,
  kBi8,
  kBi9,
  kBi10,
  kBi11,
  kBi12,
  kBi13,
  kBi14,
  kBi15,
  kBi16,
  kBi17,
  kBi18,
  kBi19,
  kBi20,
  kBi21,
  kBi22,
  kBi23,
  kBi24,
  kBi25,
  kNumQueries
};

namespace query_range {
enum Range {
  kMin = kInteractive1,
  kMax = kInteractive14
};
}

namespace update_range {
enum Range {
  kMin = kUpdate1,
  kMax = kUpdate8
};
}

namespace short_range {
enum Range {
  kMin = kShort1,
  kMax = kShort7
};
}

namespace bi_range {
enum Range {
  kMin = kBi1,
  kMax = kBi25
};
}

class QueryStatistics {
public:
  QueryStatistics();
  ~QueryStatistics() {}

  void add(QueryId id, unsigned long time, unsigned long received, unsigned long issued);
  void add(const QueryStatistics *statistics);
  void print();
  void print_detail( const char* filename );
  int total_count() const ;

private:
  class Data {
  public:
    Data()
        : count_(0), time_(0),
          min_time_(std::numeric_limits<unsigned long>::max()),
          max_time_(std::numeric_limits<unsigned long>::min()), delay_(0),
          min_delay_(std::numeric_limits<unsigned long>::max()),
          max_delay_(std::numeric_limits<unsigned long>::min()) {}
    unsigned long count_;
    unsigned long time_;
    unsigned long min_time_;
    unsigned long max_time_;
    unsigned long delay_;
    unsigned long min_delay_;
    unsigned long max_delay_;

    std::vector<unsigned long> query_time_;
    std::vector<unsigned long> query_received_;
    std::vector<unsigned long> query_issued_;
  };
  int total_count_;
  std::vector<Data> stats_;
};
}

#endif /* _QUERY_STATISTICS_H_ */

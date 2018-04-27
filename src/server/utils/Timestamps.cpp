#include "Timestamps.h"

#include <boost/date_time/posix_time/posix_time.hpp>

namespace timestamp {
unsigned long string_to_timestamp(std::string date) {
  boost::posix_time::ptime time_t_epoch(boost::gregorian::date(1970, 1, 1));
  std::replace(date.begin(), date.end(), 'T', ' ');
  date.erase(date.begin() + static_cast<long>(date.find('Z')));
  boost::posix_time::ptime pdate = boost::posix_time::time_from_string(date);
  boost::posix_time::time_duration diff = pdate - time_t_epoch;
  return static_cast<unsigned long>(diff.total_milliseconds());
}

unsigned long current() {
  boost::posix_time::ptime time_t_epoch(boost::gregorian::date(1970, 1, 1));
  boost::posix_time::ptime pdate =
      boost::posix_time::microsec_clock::universal_time();
  boost::posix_time::time_duration diff = pdate - time_t_epoch;
  return static_cast<unsigned long>(diff.total_milliseconds());
}

unsigned long current_micros() {
  boost::posix_time::ptime time_t_epoch(boost::gregorian::date(1970, 1, 1));
  boost::posix_time::ptime pdate =
      boost::posix_time::microsec_clock::universal_time();
  boost::posix_time::time_duration diff = pdate - time_t_epoch;
  return static_cast<unsigned long>(diff.total_microseconds());
}
}

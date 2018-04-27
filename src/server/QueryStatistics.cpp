#include "QueryStatistics.h"
#include <vector>
#include <stdio.h>
#include <fstream>

namespace ldbc_server {

QueryStatistics::QueryStatistics() { 
	total_count_ = 0;
	stats_.resize(kNumQueries); }

void QueryStatistics::add(QueryId id, unsigned long time, unsigned long received, unsigned long issued) {
	total_count_++;
  if (id >= kNumQueries) {
    return;
  }
  ++stats_[id].count_;
  stats_[id].query_time_.push_back(time);
  stats_[id].query_received_.push_back(received);
  stats_[id].query_issued_.push_back(issued);
  stats_[id].time_ += time;
  stats_[id].min_time_ = std::min(stats_[id].min_time_, time);
  stats_[id].max_time_ = std::max(stats_[id].max_time_, time);
  stats_[id].delay_ += issued - received;
  stats_[id].min_delay_ = std::min(stats_[id].min_delay_, issued - received);
  stats_[id].max_delay_ = std::max(stats_[id].max_delay_, issued - received);
}

void QueryStatistics::add(const QueryStatistics *statistics) {
	total_count_ += statistics->total_count_;
  for (unsigned int i = 0; i < stats_.size(); ++i) {
    stats_[i].count_ += statistics->stats_[i].count_;
    for( unsigned int j = 0; j < statistics->stats_[i].query_time_.size(); ++j ) {
      stats_[i].query_time_.push_back(statistics->stats_[i].query_time_[j]);
    }

    for( unsigned int j = 0; j < statistics->stats_[i].query_received_.size(); ++j ) {
      stats_[i].query_received_.push_back(statistics->stats_[i].query_received_[j]);
    }

    for( unsigned int j = 0; j < statistics->stats_[i].query_issued_.size(); ++j ) {
      stats_[i].query_issued_.push_back(statistics->stats_[i].query_issued_[j]);
    }

    stats_[i].time_ += statistics->stats_[i].time_;
    stats_[i].min_time_ =
        std::min(stats_[i].min_time_, statistics->stats_[i].min_time_);
    stats_[i].max_time_ =
        std::max(stats_[i].max_time_, statistics->stats_[i].max_time_);
    stats_[i].delay_ += statistics->stats_[i].delay_;
    stats_[i].min_delay_ =
        std::min(stats_[i].min_delay_, statistics->stats_[i].min_delay_);
    stats_[i].max_delay_ =
        std::max(stats_[i].max_delay_, statistics->stats_[i].max_delay_);
  }
}

void QueryStatistics::print() {
  for (unsigned int i = 0; i < stats_.size(); ++i) {
    unsigned long count = stats_[i].count_;
    if (i >= query_range::kMin && i <= query_range::kMax) {
      printf("query%u:\n", i - query_range::kMin + 1);
    } else if (i >= update_range::kMin && i <= update_range::kMax) {
      printf("update%u:\n", i - update_range::kMin + 1); 
    } else if (i >= short_range::kMin && i <= short_range::kMax) {
      printf("short%d:\n", i - short_range::kMin + 1);
    } else if (i >= bi_range::kMin && i <= bi_range::kMax) {
      printf("bi%d:\n", i - bi_range::kMin + 1);
    }

    printf("  count: %lu\n", count);
    if (count == 0) {
      continue;
    }
    printf("  min_time: %lu\n", stats_[i].min_time_);
    printf("  max_time: %lu\n", stats_[i].max_time_);
    printf("  avg_time: %f\n", static_cast<double>(stats_[i].time_) / count);
    printf("  min_delay: %lu\n", stats_[i].min_delay_);
    printf("  max_delay: %lu\n", stats_[i].max_delay_);
    printf("  avg_delay: %f\n", static_cast<double>(stats_[i].delay_) / count);
  }
}

static std::string get_query_name( unsigned int i ) {
  std::string ret;
  switch(i) {
    case kInteractive1:
      ret = std::string("LdbcQuery1");
      break;
    case kInteractive2:
      ret = std::string("LdbcQuery2");
      break;
    case kInteractive3:
      ret = std::string("LdbcQuery3");
      break;
    case kInteractive4:
      ret = std::string("LdbcQuery4");
      break;
    case kInteractive5:
      ret = std::string("LdbcQuery5");
      break;
    case kInteractive6:
      ret = std::string("LdbcQuery6");
      break;
    case kInteractive7:
      ret = std::string("LdbcQuery7");
      break;
    case kInteractive8:
      ret = std::string("LdbcQuery8");
      break;
    case kInteractive9:
      ret = std::string("LdbcQuery9");
      break;
    case kInteractive10:
      ret = std::string("LdbcQuery10");
      break;
    case kInteractive11:
      ret = std::string("LdbcQuery11");
      break;
    case kInteractive12:
      ret = std::string("LdbcQuery12");
      break;
    case kInteractive13:
      ret = std::string("LdbcQuery13");
      break;
    case kInteractive14:
      ret = std::string("LdbcQuery14");
      break;
    case kShort1:
      ret = std::string("LdbcShortQuery1PersonProfile");
      break;
    case kShort2:
      ret = std::string("LdbcShortQuery2PersonPosts");
      break;
    case kShort3:
      ret = std::string("LdbcShortQuery3PersonFriends");
      break;
    case kShort4:
      ret = std::string("LdbcShortQuery4MessageContent");
      break;
    case kShort5:
      ret = std::string("LdbcShortQuery5MessageCreator");
      break;
    case kShort6:
      ret = std::string("LdbcShortQuery6MessageForum");
      break;
    case kShort7:
      ret = std::string("LdbcShortQuery7MessageReplies");
      break;
    case kUpdate1:
      ret = std::string("LdbcUpdateQuery1");
      break;
    case kUpdate2:
      ret = std::string("LdbcUpdateQuery2");
      break;
    case kUpdate3:
      ret = std::string("LdbcUpdateQuery3");
      break;
    case kUpdate4:
      ret = std::string("LdbcUpdateQuery4");
      break;
    case kUpdate5:
      ret = std::string("LdbcUpdateQuery5");
      break;
    case kUpdate6:
      ret = std::string("LdbcUpdateQuery6");
      break;
    case kUpdate7:
      ret = std::string("LdbcUpdateQuery7");
      break;
    case kUpdate8:
      ret = std::string("LdbcUpdateQuery8");
      break;
    default:
      ret = std::string("Unknown");
      break;
  }
  return ret;
}

void QueryStatistics::print_detail( const char* filename ) {
  std::ofstream f;
  f.open(filename);
  f << "QueryId|ReceivedTime|ExecutedTime|Execution|CODE" << std::endl;
  for (unsigned int i = 0; i < stats_.size(); ++i) {
    std::string queryId = get_query_name(i);
    for (unsigned int j = 0; j < stats_[i].query_time_.size(); ++j) {
      f << queryId << "|" << stats_[i].query_received_[j] << "|" << stats_[i].query_issued_[j] << "|" << stats_[i].query_time_[j] << "|" << 0 << std::endl;
    }
  }
  f.close();
}

  int QueryStatistics::total_count() const {
    return total_count_;
  }
}

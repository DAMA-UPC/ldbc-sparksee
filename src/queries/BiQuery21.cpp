#include "Database.h"
#include "TypeCache.h"
#include "Utils.h"
#include <utils/Utils.h>
#include "snbInteractive.h"
#include <gdb/Graph.h>
#include <gdb/Objects.h>
#include <gdb/ObjectsIterator.h>
#include <gdb/Session.h>
#include <gdb/Sparksee.h>
#include <gdb/Value.h>
#include <stdio.h>
#include <string>
#include <cstring>
#include <vector>
#include <map>
#include <algorithm>
#include <boostPatch/property_tree/ptree.hpp>
#include <boostPatch/property_tree/json_parser.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/bind.hpp>
#include <time.h>

namespace gdb = sparksee::gdb;
namespace snb = sparksee::snb;
namespace ptree = boost::property_tree;

namespace bi {
namespace query21 {

typedef struct key_ {

  key_(long long id, double score)
    : person_id(id), zombie_score(score) {}

  long long person_id;
  double zombie_score;
  bool operator<(const key_ &b) const {
    if (zombie_score != b.zombie_score)
      return zombie_score > b.zombie_score;
    if (person_id != b.person_id)
      return person_id < b.person_id;
    return false;
  }
} Key;

typedef struct value_{
  value_(int zcount, int lcount) : zombie_count(zcount), like_count(lcount) {}

  int zombie_count;
  int like_count;
} Value;

struct Result {

  Result(long long id, int zombie_count, int like_count)
    : key(id, zombie_count > 0 ? static_cast<double>(zombie_count) / static_cast<double>(like_count) : 0.0)
    , value(zombie_count, like_count) {}

  Key key;
  Value value;
};

bool compare_result(const Result &lhs, const Result &rhs) {
  if (lhs.key.zombie_score != rhs.key.zombie_score) {
    return sparksee::utils::descending(lhs.key.zombie_score, rhs.key.zombie_score);
  }
  return sparksee::utils::ascending(lhs.key.person_id, rhs.key.person_id);
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Result &result) {
  gdb::Value val;
  ptree::ptree pt;
  pt.put<long long>("Person.id", result.key.person_id);
  pt.put<double>("zombieScore", result.key.zombie_score);
  pt.put<int>("zombieCount", result.value.zombie_count);
  pt.put<int>("likeCount", result.value.like_count);
  return pt;
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache,
                            const std::vector<Result> &results) {
  ptree::ptree pt;
  for (std::vector<Result>::const_iterator it = results.begin();
       it != results.end(); ++it) {
    pt.push_back(std::make_pair("", Project(graph, cache, *it)));
  }
  return pt;
}

datatypes::Buffer Execute(gdb::Session *sess, const char* country, long long end_date, int limit) {
#ifdef VERBOSE
  printf("Bi QUERY21: %s %llu\n", country, end_date);
#endif

  BEGIN_EXCEPTION

    boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
  snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

  BEGIN_TRANSACTION;

  int end_year = sparksee::utils::year(end_date);
  int end_month = sparksee::utils::month(end_date) - 1;

  gdb::Value value;
  long long country_oid = graph->FindObject(cache->place_name_t, value.SetString(sparksee::utils::to_wstring(country)));

  boost::scoped_ptr<gdb::Objects> country_cities(graph->Neighbors(country_oid, cache->is_part_of_t, gdb::Ingoing));
  boost::scoped_ptr<gdb::Objects> country_located(graph->Neighbors(country_cities.get(), cache->is_located_in_t, gdb::Ingoing));
  country_located->Intersection(boost::scoped_ptr<gdb::Objects>(graph->Select(cache->person_t)).get());

  boost::scoped_ptr<gdb::Objects> zombies(sess->NewObjects());
  std::vector<Result> intermediate_result;
  boost::scoped_ptr<gdb::ObjectsIterator> person_iterator(country_located->Iterator());
  while (person_iterator->HasNext()) {
    long long person_oid = person_iterator->Next();
    graph->GetAttribute(person_oid, cache->person_creation_date_t, value);

    if(value.GetTimestamp() < end_date) {
      int creation_year = sparksee::utils::year(value.GetTimestamp());
      int creation_month = sparksee::utils::month(value.GetTimestamp()) -1;

      int num_months = (end_year - creation_year)*12 - creation_month + end_month + 1;  

      boost::scoped_ptr<gdb::Objects> messages(graph->Neighbors(person_oid, cache->post_has_creator_t, gdb::Ingoing));
      boost::scoped_ptr<gdb::Objects> comments(graph->Neighbors(person_oid, cache->comment_has_creator_t, gdb::Ingoing));
      int message_count = messages->Count() + comments->Count();

      if(message_count / (float)num_months  < 1.0) {
        zombies->Add(person_oid);
      }
    }
  }


  boost::scoped_ptr<gdb::ObjectsIterator> zombie_iterator(zombies->Iterator());
  while(zombie_iterator->HasNext())  {
    gdb::oid_t next_zombie = zombie_iterator->Next();
    int total_likes = 0;
    int zombie_likes = 0;

    boost::scoped_ptr<gdb::Objects> messages(graph->Neighbors(next_zombie, cache->post_has_creator_t, gdb::Ingoing));
    boost::scoped_ptr<gdb::Objects> comments(graph->Neighbors(next_zombie, cache->comment_has_creator_t, gdb::Ingoing));
    messages->Union(comments.get());

    boost::scoped_ptr<gdb::Objects> likes(graph->Explode(messages.get(), cache->likes_t, gdb::Ingoing));
    boost::scoped_ptr<gdb::ObjectsIterator> iter_likes(likes->Iterator());
    while (iter_likes->HasNext()) {
      gdb::oid_t like_oid = iter_likes->Next();
      gdb::EdgeData* edge = graph->GetEdgeData(like_oid);
      gdb::oid_t liker_oid = edge->GetTail(); 
      graph->GetAttribute(liker_oid, cache->person_creation_date_t, value);

      long long liker_creation_date = value.GetTimestamp();
      if(liker_creation_date < end_date) {

        if(zombies->Exists(liker_oid)) {
          ++zombie_likes;
        }
        ++total_likes;
      }
    }
    graph->GetAttribute(next_zombie , cache->person_id_t, value);
    intermediate_result.push_back(Result(value.GetLong(), zombie_likes, total_likes));
  }

  std::sort(intermediate_result.begin(), intermediate_result.end(),
            compare_result);
  sparksee::utils::top(intermediate_result, limit);
  ptree::ptree pt = Project(*graph, *cache, intermediate_result);
  COMMIT_TRANSACTION

#ifdef VERBOSE
    printf("EXIT BI QUERY21: %s %llu %lu\n", country, end_date, intermediate_result.size());
#endif
  char* ret = snb::utils::to_json(pt);
  return datatypes::Buffer(ret, strlen(ret));

  END_EXCEPTION
}
}
}

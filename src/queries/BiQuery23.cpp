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
namespace query23 {

typedef struct key_ {

  key_(const std::string &name, int amonth)
      : country(name), month(amonth) {}

    std::string country;
    int month;
    bool operator<(const key_ &b) const {
      if (country != b.country)
        return country < b.country;
      if (month != b.month)
        return month < b.month;
      return false;
    }
} Key;

typedef struct value_{
  value_(int count) : message_count(count) {}

  int message_count;
} Value;

struct Result {

    Result(const std::string &name, int month, int count)
      : key(name, month), value(count) {}

    static Result create(std::map<Key, int>::value_type map_pair) {
        return Result(map_pair.first.country, map_pair.first.month, map_pair.second);
    }

    Key key;
    Value value;
};

bool compare_result(const Result &lhs, const Result &rhs) {
  if (lhs.value.message_count != rhs.value.message_count) {
      return sparksee::utils::descending(lhs.value.message_count, rhs.value.message_count);
  }
  return sparksee::utils::ascending(lhs.key, rhs.key);
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Result &result) {
    gdb::Value val;
    ptree::ptree pt;

    pt.put<std::string>("Country.name", result.key.country);
    pt.put<int>("month", result.key.month);
    pt.put<int>("messageCount", result.value.message_count);

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

datatypes::Buffer Execute(gdb::Session *sess, const char* country, int limit) {
#ifdef VERBOSE
    printf("Bi QUERY23: %s\n", country);
#endif

    BEGIN_EXCEPTION

    boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
    snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

    BEGIN_TRANSACTION;

    gdb::Value value;
    long long country_oid = graph->FindObject(cache->place_name_t, value.SetString(sparksee::utils::to_wstring(country)));

    boost::scoped_ptr<gdb::Objects> country_persons(snb::utils::people_from_country(country_oid, *graph.get()));
    boost::scoped_ptr<gdb::Objects> messages_from_country_persons(graph->Neighbors(country_persons.get(), cache->post_has_creator_t, gdb::Ingoing));
    boost::scoped_ptr<gdb::Objects> comments_from_country_persons(graph->Neighbors(country_persons.get(), cache->comment_has_creator_t, gdb::Ingoing));
    messages_from_country_persons->Union(comments_from_country_persons.get());
    sparksee::utils::ObjectsPtr messages_at_country(graph->Neighbors(country_oid, cache->is_located_in_t, gdb::Ingoing));
    messages_from_country_persons->Difference(messages_at_country.get());

    std::map<Key, int> holiday_map;
    boost::scoped_ptr<gdb::ObjectsIterator> message_iterator(messages_from_country_persons->Iterator());
    while (message_iterator->HasNext())
    {
        long long message_oid = message_iterator->Next();
        gdb::type_t type = graph->GetObjectType(message_oid);
        if(type== cache->post_t) {
          graph->GetAttribute(message_oid, cache->post_creation_date_t, value);
        } else {
          graph->GetAttribute(message_oid, cache->comment_creation_date_t, value);
        }
        int month = sparksee::utils::month(value.GetTimestamp());

        boost::scoped_ptr<gdb::Objects> message_country(graph->Neighbors(message_oid, cache->is_located_in_t, gdb::Outgoing));
        long long message_country_oid = message_country->Any();
        graph->GetAttribute(message_country_oid, cache->place_name_t, value);
        std::string country_name = sparksee::utils::to_string(value.GetString());
        ++holiday_map[Key(country_name, month)];
    }

    std::vector<Result> intermediate_result;
    std::transform(holiday_map.begin(), holiday_map.end(), std::back_inserter(intermediate_result), &Result::create);

    std::sort(intermediate_result.begin(), intermediate_result.end(),
              compare_result);
    sparksee::utils::top(intermediate_result, limit);
    ptree::ptree pt = Project(*graph, *cache, intermediate_result);
    COMMIT_TRANSACTION

#ifdef VERBOSE
    printf("EXIT BI QUERY23: %s %lu\n", country, intermediate_result.size());
#endif
    char* ret = snb::utils::to_json(pt);
    return datatypes::Buffer(ret, strlen(ret));

    END_EXCEPTION
}
}
}

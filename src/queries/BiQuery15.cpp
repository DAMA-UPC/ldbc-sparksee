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
namespace query15 {

typedef struct key_ {

  key_(long long id, int acount)
      : person_id(id), count(acount) {}

    long long person_id;
    int count;
    bool operator<(const key_ &b) const {
      return person_id < b.person_id;
    }
} Key;

typedef struct {
} Value;

struct Result {

    Result(long long id, int acount)
      : key(id, acount) {}

    Key key;
    Value value;
};

bool compare_result(const Result &lhs, const Result &rhs) {
  return sparksee::utils::ascending(lhs.key.person_id, rhs.key.person_id);
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Result &result) {
    gdb::Value val;
    ptree::ptree pt;

    pt.put<long long>("Person.id", result.key.person_id);
    pt.put<int>("count", result.key.count);

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
    printf("Bi QUERY15: %s\n", country);
#endif

    BEGIN_EXCEPTION

    boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
    snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

    BEGIN_TRANSACTION;

    gdb::Value value;
    long long country_oid = graph->FindObject(cache->place_name_t, value.SetString(sparksee::utils::to_wstring(country)));

    boost::scoped_ptr<gdb::Objects> country_cities(graph->Neighbors(country_oid, cache->is_part_of_t, gdb::Ingoing));
    boost::scoped_ptr<gdb::Objects> country_persons(graph->Neighbors(country_cities.get(), cache->is_located_in_t, gdb::Ingoing));

    std::vector<Result> intermediate_result;

    int count_persons = 0;
    int num_friends = 0;
    boost::scoped_ptr<gdb::ObjectsIterator> person_iterator(country_persons->Iterator());
    while (person_iterator->HasNext()) {
        long long person_oid = person_iterator->Next();
        if( graph->GetObjectType(person_oid) == cache->person_t ) {
          graph->GetAttribute(person_oid, cache->person_id_t, value);
          boost::scoped_ptr<gdb::Objects> friends(graph->Neighbors(person_oid, cache->knows_t, gdb::Any));
          friends->Intersection(country_persons.get());
          intermediate_result.push_back(Result(value.GetLong(), friends->Count()));
          num_friends += friends->Count();
          count_persons++;
        }
    }

    int avg_friends = 0;
    if(count_persons > 0 ) {
      avg_friends = num_friends / count_persons;
    }

    std::sort(intermediate_result.begin(), intermediate_result.end(),
              compare_result);


    for (unsigned int i = 0; i < intermediate_result.size();) { // TODO: Change to Lambda std::remove_if in c++11
        if (intermediate_result[i].key.count != avg_friends) {
            intermediate_result.erase(intermediate_result.begin() + i);
        } else {
            ++i;
        }
    }
    sparksee::utils::top(intermediate_result, limit);
    ptree::ptree pt = Project(*graph, *cache, intermediate_result);
    COMMIT_TRANSACTION

#ifdef VERBOSE
    printf("EXIT BI QUERY15: %s %lu %d\n", country, intermediate_result.size(), avg_friends);
#endif
    char* ret = snb::utils::to_json(pt);
    return datatypes::Buffer(ret, strlen(ret));

    END_EXCEPTION
}
}
}

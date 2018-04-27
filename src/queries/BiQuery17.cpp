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
namespace query17 {

struct Result {
    Result(int acount) : count(acount) {}
    int count;
};

bool compare_result(const Result &res_a, const Result &res_b) {
    return false;
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Result &result) {
    gdb::Value val;
    ptree::ptree pt;

    pt.put<int>("count", result.count);

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
    printf("Bi QUERY17: %s\n", country);
#endif

    BEGIN_EXCEPTION

    boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
    snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

    BEGIN_TRANSACTION;

    gdb::Value value;
    long long country_oid = graph->FindObject(cache->place_name_t, value.SetString(sparksee::utils::to_wstring(country)));

    boost::scoped_ptr<gdb::Objects> country_cities(graph->Neighbors(country_oid, cache->is_part_of_t, gdb::Ingoing));
    boost::scoped_ptr<gdb::Objects> country_persons(graph->Neighbors(country_cities.get(), cache->is_located_in_t, gdb::Ingoing));

    int count = 0;
    boost::scoped_ptr<gdb::ObjectsIterator> person_iterator(country_persons->Iterator());
    while (person_iterator->HasNext()) {
        long long person_oid = person_iterator->Next();
        boost::scoped_ptr<gdb::Objects> friends(graph->Neighbors(person_oid, cache->knows_t, gdb::Any));
        friends->Intersection(country_persons.get());
        boost::scoped_ptr<gdb::ObjectsIterator> friend_iterator(friends->Iterator());
        while (friend_iterator->HasNext()) {
            long long friend_oid = friend_iterator->Next();
            boost::scoped_ptr<gdb::Objects> fof(graph->Neighbors(friend_oid, cache->knows_t, gdb::Any));
            fof->Intersection(friends.get());
            count += fof->Count();
        }
    }
    count /= 6;

    std::vector<Result> intermediate_result;
    intermediate_result.push_back(Result(count));
    ptree::ptree pt = Project(*graph, *cache, intermediate_result);
    COMMIT_TRANSACTION

#ifdef VERBOSE
    printf("EXIT BI QUERY17: %s %lu\n", country, intermediate_result.size());
#endif
    char* ret = snb::utils::to_json(pt);
    return datatypes::Buffer(ret, strlen(ret));

    END_EXCEPTION
}
}
}

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
namespace query8{

struct Result {

    Result(std::string name, int acount)
      : tag_name(name), count(acount) {}

    static Result create(const std::map<std::string, int>::value_type &map_pair) {
        return Result(map_pair.first, map_pair.second);
    }

    std::string tag_name;
    int count;
};

bool compare_result(const Result &lhs, const Result &rhs) {
    if (lhs.count != rhs.count) {
        return sparksee::utils::descending(lhs.count, rhs.count);
    }
    return sparksee::utils::ascending(lhs.tag_name, rhs.tag_name);
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Result &result) {
    gdb::Value val;
    ptree::ptree pt;

    pt.put<std::string>("Tag.name", result.tag_name);
    pt.put<int>("count", result.count);

    return pt;
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache,
                            const std::vector<Result> &results, unsigned int limit) {
    ptree::ptree pt;
    unsigned int counter = 0;
    for (std::vector<Result>::const_iterator it = results.begin();
        it != results.end() && counter < limit; ++it, counter++) {
      pt.push_back(std::make_pair("", Project(graph, cache, *it)));
    }
    return pt;
}

datatypes::Buffer Execute(gdb::Session *sess, const char* tag, unsigned int limit) {
#ifdef VERBOSE
    printf("Bi QUERY8: %s %d\n", tag, limit);
#endif

    BEGIN_EXCEPTION

    boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
    snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

    BEGIN_TRANSACTION;

    gdb::Value value;
    long long tag_oid = graph->FindObject(cache->tag_name_t, value.SetString(sparksee::utils::to_wstring(tag)));

    boost::scoped_ptr<gdb::Objects> posts(graph->Neighbors(tag_oid, cache->has_tag_t, gdb::Ingoing));
    boost::scoped_ptr<gdb::Objects> replies(graph->Neighbors(posts.get(), cache->reply_of_t, gdb::Ingoing));


    std::map<std::string, int> tag_map;
    boost::scoped_ptr<gdb::ObjectsIterator> reply_iterator(replies->Iterator());
    while (reply_iterator->HasNext()) {
        long long reply_oid = reply_iterator->Next();
        
        boost::scoped_ptr<gdb::Objects> tags(graph->Neighbors(reply_oid, cache->has_tag_t, gdb::Outgoing));
        if (tags->Exists(tag_oid)) {
            continue;
        }
        boost::scoped_ptr<gdb::ObjectsIterator> tag_iterator(tags->Iterator());
        while (tag_iterator->HasNext()) {
            long long other_tag_oid = tag_iterator->Next();
            graph->GetAttribute(other_tag_oid, cache->tag_name_t, value);
            ++tag_map[sparksee::utils::to_string(value.GetString())];
        }
    }

    std::vector<Result> results;
    std::transform(tag_map.begin(), tag_map.end(), std::back_inserter(results), &Result::create);
    std::sort(results.begin(), results.end(), compare_result);
    ptree::ptree pt = Project(*graph, *cache, results, limit);
    COMMIT_TRANSACTION

#ifdef VERBOSE
    printf("EXIT BI QUERY8: %s %lu\n", tag, results.size());
#endif
    char* ret = snb::utils::to_json(pt);
    return datatypes::Buffer(ret, strlen(ret));

    END_EXCEPTION
}
}
}

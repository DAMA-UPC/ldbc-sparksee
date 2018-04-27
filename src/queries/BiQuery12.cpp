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
namespace query12 {

struct Result {

  Result(long long id, long long date, std::string fname, std::string lname, int count)
      : post_id(id), creation_date(date), like_count(count),
        first_name(fname), last_name(lname) {}

    long long post_id;
    long long creation_date;
    int like_count;
    std::string first_name;
    std::string last_name;
};

bool compare_result(const Result &res_a, const Result &res_b) {
    if (res_a.like_count != res_b.like_count) {
        return sparksee::utils::descending(res_a.like_count, res_b.like_count);
    }
    return sparksee::utils::ascending(res_a.post_id, res_b.post_id);
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Result &result) {
    gdb::Value val;
    ptree::ptree pt;

    pt.put<long long>("Post.id", result.post_id);
    pt.put<long long>("Post.creationDate", result.creation_date);
    pt.put<std::string>("Person.firstName", result.first_name);
    pt.put<std::string>("Person.lastName", result.last_name);
    pt.put<int>("likeCount", result.like_count);

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

datatypes::Buffer Execute(gdb::Session *sess, long long creation_date, int like_count, int limit) {
#ifdef VERBOSE
    printf("Bi QUERY12: %llu %i\n", creation_date, like_count);
#endif

    BEGIN_EXCEPTION

    boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
    snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

    BEGIN_TRANSACTION;

    gdb::Value value;

    boost::scoped_ptr<gdb::Objects> posts(graph->Select(cache->post_creation_date_t, gdb::GreaterThan, value.SetTimestamp(creation_date)));

    std::vector<Result> results;
    boost::scoped_ptr<gdb::ObjectsIterator> post_iterator(posts->Iterator());
    while (post_iterator->HasNext()) {
        long long post_oid = post_iterator->Next();

        boost::scoped_ptr<gdb::Objects> likes(graph->Neighbors(post_oid, cache->likes_t, gdb::Ingoing));
        if (likes->Count() <= like_count) {
            continue;
        }

        graph->GetAttribute(post_oid, cache->post_id_t, value);
        long long post_id = value.GetLong();
        graph->GetAttribute(post_oid, cache->post_creation_date_t, value);
        long long date = value.GetTimestamp();
        long long person_oid = boost::scoped_ptr<gdb::Objects>(graph->Neighbors(post_oid, cache->post_has_creator_t, gdb::Outgoing))->Any();

        graph->GetAttribute(person_oid, cache->person_first_name_t, value);
        std::string first_name = sparksee::utils::to_string(value.GetString());
        graph->GetAttribute(person_oid, cache->person_last_name_t, value);
        std::string last_name = sparksee::utils::to_string(value.GetString());

        results.push_back(Result(post_id, date, first_name, last_name, likes->Count()));
    }

    boost::scoped_ptr<gdb::Objects> comments(graph->Select(cache->comment_creation_date_t, gdb::GreaterThan, value.SetTimestamp(creation_date)));
    boost::scoped_ptr<gdb::ObjectsIterator> comment_iterator(comments->Iterator());
    while (comment_iterator->HasNext()) {
        long long comment_oid = comment_iterator->Next();

        boost::scoped_ptr<gdb::Objects> likes(graph->Neighbors(comment_oid, cache->likes_t, gdb::Ingoing));
        if (likes->Count() <= like_count) {
            continue;
        }

        graph->GetAttribute(comment_oid, cache->comment_id_t, value);
        long long comment_id = value.GetLong();
        graph->GetAttribute(comment_oid, cache->comment_creation_date_t, value);
        long long date = value.GetTimestamp();
        long long person_oid = boost::scoped_ptr<gdb::Objects>(graph->Neighbors(comment_oid, cache->comment_has_creator_t, gdb::Outgoing))->Any();

        graph->GetAttribute(person_oid, cache->person_first_name_t, value);
        std::string first_name = sparksee::utils::to_string(value.GetString());
        graph->GetAttribute(person_oid, cache->person_last_name_t, value);
        std::string last_name = sparksee::utils::to_string(value.GetString());
        results.push_back(Result(comment_id, date, first_name, last_name, likes->Count()));
    }
      
    std::sort(results.begin(), results.end(), compare_result);
    sparksee::utils::top(results, limit);
    ptree::ptree pt = Project(*graph, *cache, results);
    COMMIT_TRANSACTION

#ifdef VERBOSE
    printf("EXIT BI QUERY12: %llu %i %lu\n", creation_date, like_count, results.size());
#endif
    char* ret = snb::utils::to_json(pt);
    return datatypes::Buffer(ret, strlen(ret));

    END_EXCEPTION
}
}
}

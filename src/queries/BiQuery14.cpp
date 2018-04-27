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
namespace query14 {

struct Result {

  Result(long long id, std::string fname, std::string lname, int threads, int messages)
      : person_id(id), thread_count(threads), message_count(messages),
        first_name(fname), last_name(lname) {}

    long long person_id;
    int thread_count;
    int message_count;
    std::string first_name;
    std::string last_name;
};

bool compare_result(const Result &res_a, const Result &res_b) {
    if (res_a.message_count != res_b.message_count) {
        return sparksee::utils::descending(res_a.message_count, res_b.message_count);
    }
    return sparksee::utils::ascending(res_a.person_id, res_b.person_id);
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Result &result) {
    gdb::Value val;
    ptree::ptree pt;

    pt.put<long long>("Person.id", result.person_id);
    pt.put<std::string>("Person.firstName", result.first_name);
    pt.put<std::string>("Person.lastName", result.last_name);
    pt.put<int>("threadCount", result.thread_count);
    pt.put<int>("messageCount", result.message_count);

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

datatypes::Buffer Execute(gdb::Session *sess, long long date_begin, long long date_end, int limit) {
#ifdef VERBOSE
    printf("Bi QUERY14: %llu %llu\n", date_begin, date_end);
#endif

    BEGIN_EXCEPTION

    boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
    snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

    BEGIN_TRANSACTION;

    gdb::Value value;
    gdb::Value value_end;

    boost::scoped_ptr<gdb::Objects> posts(graph->Select(cache->post_creation_date_t, gdb::Between, value.SetTimestamp(date_begin),value_end.SetTimestamp(date_end)));
    boost::scoped_ptr<gdb::Objects> persons(graph->Neighbors(posts.get(), cache->post_has_creator_t, gdb::Outgoing));

    std::vector<Result> results;
    boost::scoped_ptr<gdb::ObjectsIterator> person_iterator(persons->Iterator());
    while (person_iterator->HasNext()) {
        long long person_oid = person_iterator->Next();
        graph->GetAttribute(person_oid, cache->person_id_t, value);
        long long person_id = value.GetLong();
        graph->GetAttribute(person_oid, cache->person_first_name_t, value);
        std::string first_name = sparksee::utils::to_string(value.GetString());
        graph->GetAttribute(person_oid, cache->person_last_name_t, value);
        std::string last_name = sparksee::utils::to_string(value.GetString());

        boost::scoped_ptr<gdb::Objects> person_posts(graph->Neighbors(person_oid, cache->post_has_creator_t, gdb::Ingoing));
        person_posts->Intersection(posts.get());
        
        boost::scoped_ptr<gdb::Objects> comments(graph->Neighbors(person_posts.get(), cache->reply_of_t, gdb::Ingoing));
        int num_messages = 1;
        do {
            num_messages = comments->Count();
            comments->Union(boost::scoped_ptr<gdb::Objects>(graph->Neighbors(comments.get(), cache->reply_of_t, gdb::Ingoing)).get());
        } while(num_messages != comments->Count());
        boost::scoped_ptr<gdb::Objects> final_comments(graph->Select(cache->comment_creation_date_t, gdb::Between, value.SetTimestamp(date_begin),value_end.SetTimestamp(date_end), comments.get()));
        results.push_back(Result(person_id, first_name, last_name, person_posts->Count(), final_comments->Count() + person_posts->Count()));
    }
      
    std::sort(results.begin(), results.end(), compare_result);
    sparksee::utils::top(results, limit);
    ptree::ptree pt = Project(*graph, *cache, results);
    COMMIT_TRANSACTION

#ifdef VERBOSE
    printf("EXIT BI QUERY14: %llu %llu %lu\n", date_begin, date_end, results.size());
#endif
    char* ret = snb::utils::to_json(pt);
    return datatypes::Buffer(ret, strlen(ret));

    END_EXCEPTION
}
}
}

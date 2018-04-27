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
namespace query10{

struct Result {

  Result(long long id, int ascore, int fscore)
      : person_id(id), score(ascore), friends_score(fscore) {}

    long long person_id;
    int score;
    int friends_score;
};

bool compare_result(const Result &lhs, const Result &rhs) {
    int lsum = lhs.score + lhs.friends_score;
    int rsum = rhs.score + rhs.friends_score;
    if (lsum != rsum) {
        return sparksee::utils::descending(lsum, rsum);
    }
    return sparksee::utils::ascending(lhs.person_id, rhs.person_id);
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Result &result) {
    gdb::Value val;
    ptree::ptree pt;

    pt.put<long long>("Person.id", result.person_id);
    pt.put<int>("score", result.score);
    pt.put<int>("friendsScore", result.friends_score);

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

datatypes::Buffer Execute(gdb::Session *sess, const char* tag, long long date, unsigned int limit) {
#ifdef VERBOSE
    printf("Bi QUERY10: %s %lld %u \n", tag, date, limit);
#endif

    BEGIN_EXCEPTION

    boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
    snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

    BEGIN_TRANSACTION;

    gdb::Value value;
    long long tag_oid = graph->FindObject(cache->tag_name_t, value.SetString(sparksee::utils::to_wstring(tag)));

    boost::scoped_ptr<gdb::Objects> persons(graph->Neighbors(tag_oid, cache->has_interest_t, gdb::Ingoing));
    value.SetTimestamp(date);
    boost::scoped_ptr<gdb::Objects> messages(graph->Select(cache->post_creation_date_t, gdb::GreaterThan, value));
    messages->Union(boost::scoped_ptr<gdb::Objects>(graph->Select(cache->comment_creation_date_t, gdb::GreaterThan,value)).get());
    messages->Intersection(boost::scoped_ptr<gdb::Objects>(graph->Neighbors(tag_oid, cache->has_tag_t, gdb::Ingoing)).get());
    persons->Union(boost::scoped_ptr<gdb::Objects>(graph->Neighbors(messages.get(), cache->post_has_creator_t, gdb::Outgoing)).get());
    persons->Union(boost::scoped_ptr<gdb::Objects>(graph->Neighbors(messages.get(), cache->comment_has_creator_t, gdb::Outgoing)).get());


    std::map<int, int> person_score_map;
    std::vector<Result> results;
    boost::scoped_ptr<gdb::ObjectsIterator> person_iterator(persons->Iterator());
    while (person_iterator->HasNext()) {
        long long person_oid = person_iterator->Next();
        int score = (graph->FindEdge(cache->has_interest_t, person_oid, tag_oid) != gdb::Objects::InvalidOID) ? 100 : 0;
        boost::scoped_ptr<gdb::Objects> person_messages(sess->NewObjects());
        boost::scoped_ptr<gdb::Objects> person_posts(graph->Neighbors(person_oid, cache->post_has_creator_t, gdb::Ingoing));
        boost::scoped_ptr<gdb::Objects> person_comments(graph->Neighbors(person_oid, cache->comment_has_creator_t, gdb::Ingoing));
        person_messages->Union(person_posts.get());
        person_messages->Union(person_comments.get());
        person_messages->Intersection(messages.get());
        score += person_messages->Count();
        person_score_map[person_oid] = score;
        graph->GetAttribute(person_oid, cache->person_id_t, value);
        results.push_back(Result(value.GetLong(), score, 0));
    }

    for (std::vector<Result>::iterator it = results.begin(); it != results.end(); ++it) {
        long long person_oid = graph->FindObject(cache->person_id_t, value.SetLong(it->person_id));
        boost::scoped_ptr<gdb::Objects> friends(graph->Neighbors(person_oid, cache->knows_t, gdb::Outgoing));
        boost::scoped_ptr<gdb::ObjectsIterator> friend_iterator(friends->Iterator());
        while (friend_iterator->HasNext()) {
            long long friend_oid = friend_iterator->Next();
            std::map<int, int>::iterator map_pair = person_score_map.find(friend_oid);
            if (map_pair == person_score_map.end()) {
                continue;
            }
            it->friends_score += map_pair->second;
        }
    }
    std::sort(results.begin(), results.end(), compare_result);
    sparksee::utils::top(results, 100);
    ptree::ptree pt = Project(*graph, *cache, results);
    COMMIT_TRANSACTION

#ifdef VERBOSE
    printf("EXIT BI QUERY10: %s %lu\n", tag, results.size());
#endif
    char* ret = snb::utils::to_json(pt);
    return datatypes::Buffer(ret, strlen(ret));

    END_EXCEPTION
}
}
}

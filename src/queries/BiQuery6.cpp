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
namespace query6{

struct Result {

  Result(long long id, int rcount, int lcount, int pcount, int ascore)
      : person_id(id), reply_count(rcount), like_count(lcount), post_count(pcount), score(ascore) {}

    long long person_id;
    int reply_count;
    int like_count;
    int post_count;
    int score;
};

bool compare_result(const Result &lhs, const Result &rhs) {
    if (lhs.score != rhs.score) {
        return sparksee::utils::descending(lhs.score, rhs.score);
    }
    return sparksee::utils::ascending(lhs.person_id, rhs.person_id);
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Result &result) {
    gdb::Value val;
    ptree::ptree pt;

    pt.put<long long>("Person.id", result.person_id);
    pt.put<int>("replyCount", result.reply_count);
    pt.put<int>("likeCount", result.like_count);
    pt.put<int>("postCount", result.post_count);
    pt.put<int>("score", result.score);

    return pt;
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache,
                            const std::vector<Result> &results, unsigned int limit) {
    ptree::ptree pt;
    unsigned int count = 0;
    for (std::vector<Result>::const_iterator it = results.begin();
        it != results.end() && count < limit; ++it, count++) {
      pt.push_back(std::make_pair("", Project(graph, cache, *it)));
    }
    return pt;
}

datatypes::Buffer Execute(gdb::Session *sess, const char* tag, unsigned int limit) {
#ifdef VERBOSE
    printf("Bi QUERY6: %s %d\n", tag, limit);
#endif

    BEGIN_EXCEPTION

    boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
    snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

    BEGIN_TRANSACTION;

    gdb::Value value;
    long long tag_oid = graph->FindObject(cache->tag_name_t, value.SetString(sparksee::utils::to_wstring(tag)));

    boost::scoped_ptr<gdb::Objects> messages(graph->Neighbors(tag_oid, cache->has_tag_t, gdb::Ingoing));
    boost::scoped_ptr<gdb::Objects> persons_posts(graph->Neighbors(messages.get(), cache->post_has_creator_t, gdb::Outgoing));
    boost::scoped_ptr<gdb::Objects> persons_comments(graph->Neighbors(messages.get(), cache->comment_has_creator_t, gdb::Outgoing));
    boost::scoped_ptr<gdb::Objects> persons(sess->NewObjects());
    persons->Union(persons_posts.get());
    persons->Union(persons_comments.get());


    std::vector<Result> results;
    boost::scoped_ptr<gdb::ObjectsIterator> person_iterator(persons->Iterator());
    while (person_iterator->HasNext()) {
        long long person_oid = person_iterator->Next();

        boost::scoped_ptr<gdb::Objects> person_posts(graph->Neighbors(person_oid, cache->post_has_creator_t, gdb::Ingoing));
        boost::scoped_ptr<gdb::Objects> person_comments(graph->Neighbors(person_oid, cache->comment_has_creator_t, gdb::Ingoing));
        boost::scoped_ptr<gdb::Objects> person_messages(sess->NewObjects());
        person_messages->Union(person_posts.get());
        person_messages->Union(person_comments.get());
        person_messages->Intersection(messages.get());

        int score = person_messages->Count();

        boost::scoped_ptr<gdb::Objects> replies(graph->Neighbors(person_messages.get(), cache->reply_of_t, gdb::Ingoing));
        score += (2 * replies->Count());

        boost::scoped_ptr<gdb::Objects> likes(graph->Explode(person_messages.get(), cache->likes_t, gdb::Ingoing));
        score += (10 * likes->Count());

        graph->GetAttribute(person_oid, cache->person_id_t, value);
        results.push_back(Result(value.GetLong(), replies->Count(), likes->Count(), person_messages->Count(), score));
    }

    std::sort(results.begin(), results.end(), compare_result);
    sparksee::utils::top(results, 100);
    ptree::ptree pt = Project(*graph, *cache, results, limit);
    COMMIT_TRANSACTION

#ifdef VERBOSE
    printf("EXIT BI QUERY6: %s %lu\n", tag, results.size());
#endif
    char* ret = snb::utils::to_json(pt);
    return datatypes::Buffer(ret, strlen(ret));


    END_EXCEPTION
}
}
}

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
namespace query7 {

typedef struct key_ {

  key_(long long id, int ascore)
      : person_id(id), score(ascore) {}

    long long person_id;
    int score;
    bool operator<(const key_ &b) const {
      if (score != b.score)
        return score > b.score;
      if (person_id != b.person_id)
        return person_id < b.person_id;
      return false;
    }
} Key;

typedef struct {
} Value;

struct Result {

    Result(long long id, int ascore)
      : key(id, ascore) {}

    Key key;
    Value value;
};

bool compare_result(const Result &lhs, const Result &rhs) {
  if (lhs.key.score != rhs.key.score) {
      return sparksee::utils::descending(lhs.key.score, rhs.key.score);
  }
  return sparksee::utils::ascending(lhs.key.person_id, rhs.key.person_id);
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Result &result) {
    gdb::Value val;
    ptree::ptree pt;

    pt.put<long long>("Person.id", result.key.person_id);
    pt.put<int>("score", result.key.score);

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
    printf("Bi QUERY7: %s %d\n", tag, limit);
#endif

    BEGIN_EXCEPTION

    boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
    snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

    BEGIN_TRANSACTION;

    gdb::Value value;
    long long tag_oid = graph->FindObject(cache->tag_name_t, value.SetString(sparksee::utils::to_wstring(tag)));

    std::vector<Result> intermediate_result;

    boost::scoped_ptr<gdb::Objects> tag_messages(sess->NewObjects());
    boost::scoped_ptr<gdb::Objects> tag_posts(graph->Neighbors(tag_oid, cache->post_has_tag_t, gdb::Ingoing));
    boost::scoped_ptr<gdb::Objects> tag_comments(graph->Neighbors(tag_oid, cache->comment_has_tag_t, gdb::Ingoing));
    tag_messages->Union(tag_posts.get());
    tag_messages->Union(tag_comments.get());

    boost::scoped_ptr<gdb::Objects> tag_persons(sess->NewObjects());
    boost::scoped_ptr<gdb::Objects> tag_posts_persons(graph->Neighbors(tag_posts.get(), cache->post_has_creator_t, gdb::Outgoing));
    boost::scoped_ptr<gdb::Objects> tag_comments_persons(graph->Neighbors(tag_comments.get(), cache->comment_has_creator_t, gdb::Outgoing));
    tag_persons->Union(tag_posts_persons.get());
    tag_persons->Union(tag_comments_persons.get());


    /*std::map<gdb::oid_t, long> likes_per_message;
    boost::scoped_ptr<gdb::Objects> likes(graph->Select(cache->likes_t));
    boost::scoped_ptr<gdb::ObjectsIterator> iter_likes(likes->Iterator());
    while (iter_likes->HasNext()) {
      gdb::oid_t like = iter_likes->Next();
      likes_per_message[graph->GetEdgeData(like)->GetHead()]++;
      likes_per_message[graph->GetEdgeData(like)->GetTail()]++;
    }

    std::map<gdb::oid_t, long> popularity;
    for (std::map<gdb::oid_t,long>::iterator it = likes_per_message.begin(); it != likes_per_message.end(); ++it) {
      if(graph->GetObjectType(it->first) == cache->post_t) {
        boost::scoped_ptr<gdb::Objects> creator(graph->Neighbors(it->first, cache->post_has_creator_t, gdb::Outgoing));
        popularity[creator->Any()]++;
      } else if(graph->GetObjectType(it->first) == cache->comment_t){
        boost::scoped_ptr<gdb::Objects> creator(graph->Neighbors(it->first, cache->comment_has_creator_t, gdb::Outgoing));
        popularity[creator->Any()]++;
      }
    }*/

    std::map<gdb::oid_t, long> popularity;
    boost::scoped_ptr<gdb::ObjectsIterator> person_iterator(tag_persons->Iterator());
    while (person_iterator->HasNext()) {
        long long person_oid = person_iterator->Next();
        graph->GetAttribute(person_oid, cache->person_id_t, value);

        boost::scoped_ptr<gdb::Objects> person_messages(sess->NewObjects());
        boost::scoped_ptr<gdb::Objects> person_posts(graph->Neighbors(person_oid, cache->post_has_creator_t, gdb::Ingoing));
        boost::scoped_ptr<gdb::Objects> person_comments(graph->Neighbors(person_oid, cache->comment_has_creator_t, gdb::Ingoing));
        person_messages->Union(person_posts.get());
        person_messages->Union(person_comments.get());
        person_messages->Intersection(tag_messages.get()); 

        boost::scoped_ptr<gdb::Objects> likers(graph->Neighbors(person_messages.get(), cache->likes_t, gdb::Ingoing));
        int auth_score = 0;
        boost::scoped_ptr<gdb::ObjectsIterator> iter_likers(likers->Iterator());
        while(iter_likers->HasNext()) {
          gdb::oid_t liker = iter_likers->Next();

          if(popularity.find(liker) == popularity.end()) {
            boost::scoped_ptr<gdb::Objects> liker_posts(graph->Neighbors(liker,cache->post_has_creator_t, gdb::Ingoing));
            boost::scoped_ptr<gdb::Objects> liker_posts_likes(graph->Explode(liker_posts.get(), cache->likes_t, gdb::Ingoing));

            boost::scoped_ptr<gdb::Objects> liker_comments(graph->Neighbors(liker,cache->comment_has_creator_t, gdb::Ingoing));
            boost::scoped_ptr<gdb::Objects> liker_comments_likes(graph->Explode(liker_comments.get(), cache->likes_t, gdb::Ingoing));
            popularity[liker] = liker_posts_likes->Count() + liker_comments_likes->Count();
          }
          auth_score += popularity[liker];
        }
        intermediate_result.push_back(Result(value.GetLong(), auth_score));
    }

    std::sort(intermediate_result.begin(), intermediate_result.end(),
              compare_result);
    sparksee::utils::top(intermediate_result, 100);
    ptree::ptree pt = Project(*graph, *cache, intermediate_result, limit);
    COMMIT_TRANSACTION

#ifdef VERBOSE
    printf("EXIT BI QUERY7: %s %lu\n", tag, intermediate_result.size());
#endif
    char *ret = snb::utils::to_json(pt);
    return  datatypes::Buffer(ret, strlen(ret));

    END_EXCEPTION
}
}
}

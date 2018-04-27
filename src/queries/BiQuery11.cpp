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
namespace query11 {

typedef struct key_ {

  key_(long long id, const std::string &tag)
      : person_id(id), tag_name(tag) {}

    long long person_id;
    std::string tag_name;
    bool operator<(const key_ &b) const {
      if (person_id != b.person_id)
        return sparksee::utils::descending<unsigned long>(person_id,b.person_id);
      return sparksee::utils::ascending<std::string>(tag_name,b.tag_name);
    }
} Key;

typedef struct value_{
  value_() : like_count(0), reply_count(0) {}
  value_(int l_count, int r_count) : like_count(l_count), reply_count(r_count) {}
  int like_count;
  int reply_count;
} Value;

struct Result {

    Result(long long id, const std::string &name, int l_count, int r_count)
      : key(id, name), value(l_count,r_count) {}

    static Result create(std::map<Key, Value>::value_type map_pair) {
        return Result(map_pair.first.person_id, map_pair.first.tag_name, map_pair.second.like_count, map_pair.second.reply_count);
    }

    Key key;
    Value value;
};

bool compare_result(const Result &lhs, const Result &rhs) {
  if ((lhs.value.like_count) != (rhs.value.like_count)) {
      return sparksee::utils::descending((lhs.value.like_count), (rhs.value.like_count));
  }
  if (lhs.key.person_id != rhs.key.person_id)
    return sparksee::utils::ascending(lhs.key.person_id, rhs.key.person_id);
  return sparksee::utils::ascending<std::string>(lhs.key.tag_name,rhs.key.tag_name);
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Result &result) {
    gdb::Value val;
    ptree::ptree pt;
    pt.put<long long>("Person.id", result.key.person_id);
    pt.put<std::string>("Tag.name", result.key.tag_name);
    pt.put<int>("countLikes", result.value.like_count);
    pt.put<int>("countReplies", result.value.reply_count);
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

datatypes::Buffer Execute(gdb::Session *sess, const char* country, const char** blacklist, int blacklist_size, int limit) {
#ifdef VERBOSE
    printf("Bi QUERY11: %s %i\n", country, blacklist_size);
#endif

    BEGIN_EXCEPTION

    boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
    snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

    BEGIN_TRANSACTION;

    gdb::Value value;
    long long country_oid = graph->FindObject(cache->place_name_t, value.SetString(sparksee::utils::to_wstring(country)));

    boost::scoped_ptr<gdb::Objects> country_cities(graph->Neighbors(country_oid, cache->is_part_of_t, gdb::Ingoing));
    boost::scoped_ptr<gdb::Objects> country_located(graph->Neighbors(country_cities.get(), cache->is_located_in_t, gdb::Ingoing));
    boost::scoped_ptr<gdb::Objects> comments(graph->Neighbors(country_located.get(), cache->comment_has_creator_t, gdb::Ingoing));

    std::map<Key, Value> count_map;
    boost::scoped_ptr<gdb::ObjectsIterator> comment_iterator(comments->Iterator());
    while (comment_iterator->HasNext()) {
        long long comment_oid = comment_iterator->Next();

        boost::scoped_ptr<gdb::Objects> replied_post(graph->Neighbors(comment_oid, cache->reply_of_t, gdb::Outgoing));
        long long replied_oid = replied_post->Any();
        boost::scoped_ptr<gdb::Objects> comment_tags(graph->Neighbors(comment_oid, cache->has_tag_t, gdb::Outgoing));
        boost::scoped_ptr<gdb::Objects> replied_tags(graph->Neighbors(replied_oid, cache->has_tag_t, gdb::Outgoing));
        replied_tags->Intersection(comment_tags.get());
        if (replied_tags->Count() > 0) {
            continue;
        }
        graph->GetAttribute(comment_oid, cache->comment_content_t, value);
        std::string content = sparksee::utils::to_string(value.GetString());
        bool contains_blacklist = false;
        for (int i = 0; i < blacklist_size && !contains_blacklist; ++i) {
            contains_blacklist = (content.find(blacklist[i]) != std::string::npos);
        }
        if (contains_blacklist) {
          continue;
        }
        boost::scoped_ptr<gdb::Objects> likes(graph->Neighbors(comment_oid, cache->likes_t, gdb::Ingoing));
        boost::scoped_ptr<gdb::Objects> creator(graph->Neighbors(comment_oid, cache->comment_has_creator_t, gdb::Outgoing));
        long long creator_oid = creator->Any();
        gdb::Value person_value;
        graph->GetAttribute(creator_oid, cache->person_id_t, person_value);
        boost::scoped_ptr<gdb::ObjectsIterator> tag_iterator(comment_tags->Iterator());
        while (tag_iterator->HasNext()) {
            long long tag_oid = tag_iterator->Next();
            graph->GetAttribute(tag_oid, cache->tag_name_t, value);
            ++count_map[Key(person_value.GetLong(), sparksee::utils::to_string(value.GetString()))].reply_count;
            count_map[Key(person_value.GetLong(), sparksee::utils::to_string(value.GetString()))].like_count += likes->Count();;
        } 
    }

    std::vector<Result> intermediate_result;
    std::transform(count_map.begin(), count_map.end(), std::back_inserter(intermediate_result), &Result::create);

    std::sort(intermediate_result.begin(), intermediate_result.end(), compare_result);
    sparksee::utils::top(intermediate_result, limit);
    ptree::ptree pt = Project(*graph, *cache, intermediate_result);
    COMMIT_TRANSACTION

#ifdef VERBOSE
    printf("EXIT BI QUERY11: %s %i %lu\n", country, blacklist_size, intermediate_result.size());
#endif
    char* ret = snb::utils::to_json(pt);
    return datatypes::Buffer(ret, strlen(ret)); 

    END_EXCEPTION
}
}
}

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
namespace query9 {

typedef struct key_ {

  key_(long long id, int adiff)
      : forum_id(id), diff(adiff) {}

    long long forum_id;
    int diff;
    bool operator<(const key_ &b) const {
      if (diff != b.diff)
        return diff > b.diff;
      if (forum_id != b.forum_id)
        return forum_id < b.forum_id;
      return false;
    }
} Key;

typedef struct value_ {
    value_ (int acount1, int acount2) 
      : count1(acount1)
      , count2(acount2) {}

    int count1;
    int count2;
} Value;

struct Result {

    Result(long long id, int acount1, int acount2)
      : key(id, std::abs(acount2 - acount1)), value(acount1, acount2)  {}

    Key key;
    Value value;
};

bool compare_result(const Result &lhs, const Result &rhs) {
  if (lhs.key.diff != rhs.key.diff) {
      return sparksee::utils::descending(lhs.key.diff, rhs.key.diff);
  }
  return sparksee::utils::ascending(lhs.key.forum_id, rhs.key.forum_id);
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Result &result) {
    gdb::Value val;
    ptree::ptree pt;

    pt.put<long long>("Forum.id", result.key.forum_id);
    pt.put<int>("count1", result.value.count1);
    pt.put<int>("count2", result.value.count2);

    return pt;
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache,
                            const std::vector<Result> &results, int limit) {
    ptree::ptree pt;
    int count = 0;
    for (std::vector<Result>::const_iterator it = results.begin();
        it != results.end() && count < limit; ++it, count++) {
      pt.push_back(std::make_pair("", Project(graph, cache, *it)));
    }
    return pt;
}

datatypes::Buffer Execute(gdb::Session *sess, const char* tag_class1, const char* tag_class2, int threshold, int limit) {
#ifdef VERBOSE
    printf("Bi QUERY9: %s %s %i\n", tag_class1, tag_class2, threshold);
#endif

    BEGIN_EXCEPTION

    boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
    snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

    BEGIN_TRANSACTION;

    gdb::Value value;
    long long class1_oid = graph->FindObject(cache->tagclass_name_t, value.SetString(sparksee::utils::to_wstring(tag_class1)));
    long long class2_oid = graph->FindObject(cache->tagclass_name_t, value.SetString(sparksee::utils::to_wstring(tag_class2)));

    boost::scoped_ptr<gdb::Objects> class1_tags(graph->Neighbors(class1_oid, cache->has_type_t, gdb::Ingoing ));
    boost::scoped_ptr<gdb::Objects> class2_tags(graph->Neighbors(class2_oid, cache->has_type_t, gdb::Ingoing ));

    boost::scoped_ptr<gdb::Objects> class1_posts(graph->Neighbors(class1_tags.get(), cache->has_tag_t, gdb::Ingoing));
    boost::scoped_ptr<gdb::Objects> class2_posts(graph->Neighbors(class2_tags.get(), cache->has_tag_t, gdb::Ingoing));

    std::vector<Result> intermediate_result;
    
    boost::scoped_ptr<gdb::Objects> forums(graph->Select(cache->forum_t));
    boost::scoped_ptr<gdb::ObjectsIterator> forum_iterator(forums->Iterator());
    while (forum_iterator->HasNext()) {
        long long forum_oid = forum_iterator->Next();
        boost::scoped_ptr<gdb::Objects> members(graph->Explode(forum_oid, cache->has_member_t, gdb::Outgoing));
        if (members->Count() <= threshold) {
            continue;
        }
        graph->GetAttribute(forum_oid, cache->forum_id_t, value);
        boost::scoped_ptr<gdb::Objects> forum_posts(graph->Neighbors(forum_oid, cache->container_of_t, gdb::Outgoing));
        boost::scoped_ptr<gdb::Objects> forum_class1_posts(
            gdb::Objects::CombineIntersection(forum_posts.get(),
                                              class1_posts.get()));
        boost::scoped_ptr<gdb::Objects> forum_class2_posts(
            gdb::Objects::CombineIntersection(forum_posts.get(),
                                              class2_posts.get()));
        if(forum_class1_posts->Count() > 0 && forum_class2_posts->Count() > 0)
          intermediate_result.push_back(Result(value.GetLong(), forum_class1_posts->Count(), forum_class2_posts->Count()));
    }

    std::sort(intermediate_result.begin(), intermediate_result.end(),
              compare_result);
    ptree::ptree pt = Project(*graph, *cache, intermediate_result, limit);
    COMMIT_TRANSACTION

#ifdef VERBOSE
    printf("EXIT Bi QUERY9: %s %s %i %lu\n", tag_class1, tag_class2, threshold, intermediate_result.size());
#endif
    char* ret = snb::utils::to_json(pt);
    return datatypes::Buffer(ret, strlen(ret));

    END_EXCEPTION
}
}
}

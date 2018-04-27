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
namespace query4 {


struct Result {
  long long id_;
  std::string title_;
  long long creation_date_;
  long long moderator_id_;
  unsigned int count_;
};

bool compare_result(const Result &res_a, const Result &res_b) {
    if( res_a.count_ != res_b.count_) 
      return res_a.count_ > res_b.count_;
    return res_a.id_ < res_b.id_;
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Result &result) {
    gdb::Value val;
    ptree::ptree pt;

    pt.put<long long>("Forum.id", result.id_);
    pt.put<std::string>("Forum.title", result.title_);
    pt.put<long long>("Forum.creationDate", result.creation_date_);
    pt.put<long long>("Forum.moderator", result.moderator_id_);
    pt.put<unsigned int>("Forum.count", result.count_);

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

datatypes::Buffer Execute(gdb::Session *sess, const char* tag_class, const char* country, unsigned int limit) {
#ifdef VERBOSE
    printf("Bi QUERY4: %s %s %d\n", tag_class, country, limit);
#endif

    BEGIN_EXCEPTION

    boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
    snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

    BEGIN_TRANSACTION;

    gdb::Value value; 
    gdb::oid_t country_id = graph->FindObject(cache->place_name_t , value.SetString(sparksee::utils::to_wstring(country)));
    boost::scoped_ptr<gdb::Objects> cities(graph->Neighbors(country_id, cache->is_part_of_t, gdb::Ingoing  ));
    boost::scoped_ptr<gdb::Objects> creators(graph->Neighbors(cities.get(), cache->is_located_in_t, gdb::Ingoing));
    boost::scoped_ptr<gdb::Objects> persons(graph->Select(cache->person_t));
    creators->Intersection(persons.get());


    boost::scoped_ptr<gdb::Objects> forums(graph->Neighbors(creators.get(), cache->has_moderator_t, gdb::Ingoing));
    boost::scoped_ptr<gdb::Objects> posts(graph->Neighbors(forums.get(), cache->container_of_t , gdb::Outgoing ));


    gdb::oid_t tag_class_id = graph->FindObject(cache->tagclass_name_t, value.SetString(sparksee::utils::to_wstring(tag_class)));
    boost::scoped_ptr<gdb::Objects> tags(graph->Neighbors(tag_class_id, cache->has_type_t, gdb::Ingoing ));
    boost::scoped_ptr<gdb::Objects> posts_tags(graph->Neighbors(tags.get(), cache->post_has_tag_t, gdb::Ingoing ));
    posts->Intersection(posts_tags.get());


    std::vector<Result> intermediate_result;
    boost::scoped_ptr<gdb::Objects> final_forums(graph->Neighbors(posts.get(), cache->container_of_t, gdb::Ingoing ));
    {
      boost::scoped_ptr<gdb::ObjectsIterator> iterator(final_forums->Iterator());
      while(iterator->HasNext()) {
        gdb::oid_t forum = iterator->Next();
        boost::scoped_ptr<gdb::Objects> forum_posts(graph->Neighbors(forum, cache->container_of_t, gdb::Outgoing ));
        forum_posts->Intersection(posts.get());
        if(forum_posts->Count() > 0) {
          // guardar resultat
          Result result;
          graph->GetAttribute(forum, cache->forum_id_t, value);
          result.id_ = value.GetLong();
          graph->GetAttribute(forum, cache->forum_creation_date_t , value);
          result.creation_date_ = value.GetTimestamp();
          graph->GetAttribute(forum, cache->forum_title_t , value);
          result.title_ = sparksee::utils::to_string(value.GetString());
          boost::scoped_ptr<gdb::Objects> moderator(graph->Neighbors(forum, cache->has_moderator_t, gdb::Outgoing ));
          graph->GetAttribute(moderator->Any(), cache->person_id_t, value);
          result.moderator_id_ = value.GetLong();
          result.count_ = forum_posts->Count();
          intermediate_result.push_back(result);
        }
      } 
    }

    std::sort(intermediate_result.begin(), intermediate_result.end(),
              compare_result);
    ptree::ptree pt = Project(*graph, *cache, intermediate_result, limit);
    COMMIT_TRANSACTION

#ifdef VERBOSE
    printf("EXIT BI QUERY4: %s %s %lu\n", tag_class, country ,
           intermediate_result.size());
#endif
    char* ret = snb::utils::to_json(pt);
    return datatypes::Buffer(ret,strlen(ret));

    END_EXCEPTION
}
}
}

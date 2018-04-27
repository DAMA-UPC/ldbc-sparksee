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
namespace query13 {

typedef struct key_ {

  key_(int ayear, int amonth)
      : year(ayear), month(amonth) {}

    int year;
    int month;
    bool operator<(const key_ &b) const {
      if (year != b.year)
        return year > b.year;
      return month < b.month;
    }
} Key;

typedef struct value_{

  std::map<std::string, int> tags;
} Value;


struct TagPopularity {
    static std::pair<std::string, int> create(std::map<std::string, int>::value_type map_element) {
        return std::make_pair(map_element.first, map_element.second);
    }
    static bool comparer(const std::pair<std::string, int> &lhs, const std::pair<std::string, int> &rhs) {
        //return std::get<1>(lhs) < std::get<1>(rhs);
        if( lhs.second != rhs.second )
          return sparksee::utils::descending<int>(lhs.second,rhs.second);
        return sparksee::utils::ascending<std::string>(lhs.first,rhs.first);
    }
};

static ptree::ptree Project(const gdb::Graph &graph,
    const snb::TypeCache &cache, const Key &key, const Value &value) {
  gdb::Value val;
  ptree::ptree pt;

  pt.put<int>("year", key.year);
  pt.put<int>("month", key.month);

  std::vector<std::pair<std::string, int> > tags;
  std::transform(value.tags.begin(), value.tags.end(), std::back_inserter(tags), &TagPopularity::create);
  std::sort(tags.begin(), tags.end(), &TagPopularity::comparer);
  sparksee::utils::top(tags, 6);
  ptree::ptree tag_pt;
  int counter = 0;
  for (std::vector<std::pair<std::string, int> >::iterator it = tags.begin(); it != tags.end(); ++it) {
    ptree::ptree pair_tree;
    if((*it).first != "NULL") {
      pair_tree.put<int>("popularity",(*it).second);
      pair_tree.put<std::string>("name", (*it).first);
      tag_pt.push_back(std::make_pair("",pair_tree));
      counter++;
      if(counter >= 5) break;
    }
  }
  pt.push_back(std::make_pair("tags", tag_pt));

  return pt;
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache,
                            const std::map<Key, Value> &results) {
    ptree::ptree pt;
    for (std::map<Key, Value>::const_iterator it = results.begin();
        it != results.end(); ++it) {
      pt.push_back(std::make_pair("", Project(graph, cache, it->first, it->second)));
    }
    return pt;
}

datatypes::Buffer Execute(gdb::Session *sess, const char* country, int limit) {
#ifdef VERBOSE
    printf("Bi QUERY13: %s \n", country);
#endif

    BEGIN_EXCEPTION

    boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
    snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

    BEGIN_TRANSACTION;

    gdb::Value value;
    long long country_oid = graph->FindObject(cache->place_name_t, value.SetString(sparksee::utils::to_wstring(country)));

    boost::scoped_ptr<gdb::Objects> country_located(graph->Neighbors(country_oid, cache->is_located_in_t, gdb::Ingoing));
    boost::scoped_ptr<gdb::Objects> country_located_posts(gdb::Objects::CombineIntersection(country_located.get(), boost::scoped_ptr<gdb::Objects>(graph->Select(cache->post_t)).get()));
    boost::scoped_ptr<gdb::Objects> country_located_comments(gdb::Objects::CombineIntersection(country_located.get(), boost::scoped_ptr<gdb::Objects>(graph->Select(cache->comment_t)).get()));

    std::map<Key, Value> month_map;
    boost::scoped_ptr<gdb::ObjectsIterator> post_iterator(country_located_posts->Iterator());
    while (post_iterator->HasNext()) {
        long long post_oid = post_iterator->Next();
        graph->GetAttribute(post_oid, cache->post_creation_date_t, value);
        int year = sparksee::utils::year(value.GetTimestamp());
        int month = sparksee::utils::month(value.GetTimestamp());
        boost::scoped_ptr<gdb::Objects> tags(graph->Neighbors(post_oid, cache->has_tag_t, gdb::Outgoing));
        if (tags->Count() == 0) {
            month_map[Key(year, month)].tags["NULL"] += 1;
            continue;
        }
        boost::scoped_ptr<gdb::ObjectsIterator> tag_iterator(tags->Iterator());
        while (tag_iterator->HasNext()) {
            long long tag_oid = tag_iterator->Next();
            graph->GetAttribute(tag_oid, cache->tag_name_t, value);
            month_map[Key(year, month)].tags[sparksee::utils::to_string(value.GetString())] += 1;
        }
    }


    boost::scoped_ptr<gdb::ObjectsIterator> comment_iterator(country_located_comments->Iterator());
    while (comment_iterator->HasNext()) {
        long long comment_oid = comment_iterator->Next();
        graph->GetAttribute(comment_oid, cache->comment_creation_date_t, value);
        int year = sparksee::utils::year(value.GetTimestamp());
        int month = sparksee::utils::month(value.GetTimestamp());
        boost::scoped_ptr<gdb::Objects> tags(graph->Neighbors(comment_oid, cache->has_tag_t, gdb::Outgoing));
        if (tags->Count() == 0) {
            month_map[Key(year, month)].tags["NULL"] += 1;
            continue;
        }
        boost::scoped_ptr<gdb::ObjectsIterator> tag_iterator(tags->Iterator());
        while (tag_iterator->HasNext()) {
            long long tag_oid = tag_iterator->Next();
            graph->GetAttribute(tag_oid, cache->tag_name_t, value);
            month_map[Key(year, month)].tags[sparksee::utils::to_string(value.GetString())] += 1;
        }
    }

    ptree::ptree pt = Project(*graph, *cache, month_map);
    COMMIT_TRANSACTION

#ifdef VERBOSE
    printf("EXIT BI QUERY13: %s %lu\n", country, month_map.size());
#endif
    char* ret = snb::utils::to_json(pt);
    return datatypes::Buffer(ret, strlen(ret));

    END_EXCEPTION
}
}
}

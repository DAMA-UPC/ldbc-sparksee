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
namespace query3 {

typedef struct key_ {
    std::string tag_name;
    int diff;
    key_() : diff(0) {}

    bool operator<(const key_ &b) const {
      if (diff != b.diff)
        return sparksee::utils::descending(std::abs(diff), std::abs(b.diff));
      if (tag_name != b.tag_name)
        return tag_name.compare(b.tag_name) < 0;
      return false;
    }
} Key;

typedef struct value_ {
    int count_month1;
    int count_month2;
    value_() : count_month1(0), count_month2(0) {} 
    
} Value;

struct Result {
    Key key;
    Value value;
};

bool compare_result(const Result &lhs, const Result &rhs) {
  if (std::abs(lhs.key.diff) != std::abs(rhs.key.diff)) {
      return sparksee::utils::descending(std::abs(lhs.key.diff), std::abs(rhs.key.diff));
  }
  return lhs.key.tag_name.compare(rhs.key.tag_name) < 0;
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Result &result) {
    gdb::Value val;
    ptree::ptree pt;

    pt.put<std::string>("Tag.name", result.key.tag_name);
    pt.put<int>("countInterval1", result.value.count_month1);
    pt.put<int>("countInterval2", result.value.count_month2);
    pt.put<int>("diff", std::abs(result.key.diff));

    return pt;
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache,
                            const std::vector<Result> &results, int limit) {
    ptree::ptree pt;
    int count = 0;
    for (std::vector<Result>::const_iterator it = results.begin();
        it != results.end() && count < limit; ++it, ++count) {
      pt.push_back(std::make_pair("", Project(graph, cache, *it)));
    }
    return pt;
}



datatypes::Buffer Execute(gdb::Session *sess, unsigned int year, unsigned int month, unsigned int limit) {
#ifdef VERBOSE
    printf("Bi QUERY3: %u %u %u\n", year, month, limit);
#endif

    BEGIN_EXCEPTION

    boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
    snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

    BEGIN_TRANSACTION;


    long long date_start_1 = snb::utils::date(year, month, 1);
    long long date_end_1 = 0;
    if(month < 12) {
      date_end_1 = snb::utils::date(year, month + 1, 1);
    } else {
      date_end_1 = snb::utils::date(year+1, 1, 1);
    }

    long long date_start_2 = date_end_1;
    long long date_end_2 = 0;
    if(month < 11) {
      date_end_2 = snb::utils::date(year, month + 2, 1);
    } else {
      date_end_2 = snb::utils::date(year+1, (month + 2) % 12 , 1);
    }

    sparksee::gdb::Value start_1, end_1, start_2, end_2;
    start_1.SetTimestamp(date_start_1);
    end_1.SetTimestamp(date_end_1-1);
    start_2.SetTimestamp(date_start_2);
    end_2.SetTimestamp(date_end_2-1);

    boost::scoped_ptr<gdb::Objects> prev_posts(
        graph->Select(cache->post_creation_date_t, sparksee::gdb::Between,
                      start_1, end_1));

    boost::scoped_ptr<gdb::Objects> posts(
        graph->Select(cache->post_creation_date_t, sparksee::gdb::Between,
                      start_2, end_2));


    std::map<std::string, Result> tag_map;
    {
        boost::scoped_ptr<gdb::ObjectsIterator> iterator(prev_posts->Iterator());
        while (iterator->HasNext()) {
            long long oid = iterator->Next();
            boost::scoped_ptr<gdb::Objects> tags(graph->Neighbors(oid, cache->has_tag_t, gdb::Outgoing));
            sparksee::gdb::Value name_value;
            boost::scoped_ptr<gdb::ObjectsIterator> tag_iterator(tags->Iterator());
            while (tag_iterator->HasNext()) {
                long long tag_oid = tag_iterator->Next();
                graph->GetAttribute(tag_oid, cache->tag_name_t, name_value);
                std::string name = sparksee::utils::to_string(name_value.GetString());
                tag_map[name].key.tag_name = name;
                ++tag_map[name].key.diff;
                ++tag_map[name].value.count_month1;
            }
        }
    }

    {
        boost::scoped_ptr<gdb::ObjectsIterator> iterator(posts->Iterator());
        while (iterator->HasNext()) {
            long long oid = iterator->Next();
            boost::scoped_ptr<gdb::Objects> tags(graph->Neighbors(oid, cache->has_tag_t, gdb::Outgoing));
            sparksee::gdb::Value name_value;
            boost::scoped_ptr<gdb::ObjectsIterator> tag_iterator(tags->Iterator());
            while (tag_iterator->HasNext()) {
                long long tag_oid = tag_iterator->Next();
                graph->GetAttribute(tag_oid, cache->tag_name_t, name_value);
                std::string name = sparksee::utils::to_string(name_value.GetString());
                tag_map[name].key.tag_name = name;
                --tag_map[name].key.diff;
                ++tag_map[name].value.count_month2;
            }
        }
    }

    boost::scoped_ptr<gdb::Objects> prev_comments(
        graph->Select(cache->comment_creation_date_t, sparksee::gdb::Between,
                      start_1, end_1));

    boost::scoped_ptr<gdb::Objects> comments(
        graph->Select(cache->comment_creation_date_t, sparksee::gdb::Between,
                      start_2, end_2));

    {
        boost::scoped_ptr<gdb::ObjectsIterator> iterator(prev_comments->Iterator());
        while (iterator->HasNext()) {
            long long oid = iterator->Next();
            boost::scoped_ptr<gdb::Objects> tags(graph->Neighbors(oid, cache->has_tag_t, gdb::Outgoing));
            sparksee::gdb::Value name_value;
            boost::scoped_ptr<gdb::ObjectsIterator> tag_iterator(tags->Iterator());
            while (tag_iterator->HasNext()) {
                long long tag_oid = tag_iterator->Next();
                graph->GetAttribute(tag_oid, cache->tag_name_t, name_value);
                std::string name = sparksee::utils::to_string(name_value.GetString());
                tag_map[name].key.tag_name = name;
                ++tag_map[name].key.diff;
                ++tag_map[name].value.count_month1;
            }
        }
    }

    {
        boost::scoped_ptr<gdb::ObjectsIterator> iterator(comments->Iterator());
        while (iterator->HasNext()) {
            long long oid = iterator->Next();
            boost::scoped_ptr<gdb::Objects> tags(graph->Neighbors(oid, cache->has_tag_t, gdb::Outgoing));
            sparksee::gdb::Value name_value;
            boost::scoped_ptr<gdb::ObjectsIterator> tag_iterator(tags->Iterator());
            while (tag_iterator->HasNext()) {
                long long tag_oid = tag_iterator->Next();
                graph->GetAttribute(tag_oid, cache->tag_name_t, name_value);
                std::string name = sparksee::utils::to_string(name_value.GetString());
                tag_map[name].key.tag_name = name;
                --tag_map[name].key.diff;
                ++tag_map[name].value.count_month2;
            }
        }
    }


    std::vector<Result> intermediate_result;
    std::transform(tag_map.begin(), tag_map.end(),
                   std::back_inserter(intermediate_result),
                   boost::bind(&std::map<std::string, Result>::value_type::second, _1));
    std::sort(intermediate_result.begin(), intermediate_result.end(),
              compare_result);
    ptree::ptree pt = Project(*graph, *cache, intermediate_result, limit);
    COMMIT_TRANSACTION

#ifdef VERBOSE
    printf("EXIT BI QUERY3: %u %u %lu \n", year, month, intermediate_result.size());
#endif
    char* ret = snb::utils::to_json(pt);
    return datatypes::Buffer(ret, strlen(ret));

    END_EXCEPTION
}
}
}

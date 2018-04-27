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
namespace query24 {

typedef struct key_ {

  key_(int ayear, int amonth, std::string acontinent)
      : year(ayear), month(amonth), continent(acontinent) {}

    int year;
    int month;
    std::string continent;
    bool operator<(const key_ &b) const {
      if (year != b.year)
        return sparksee::utils::ascending<int>(year,b.year);
      if (month != b.month)
        return sparksee::utils::ascending<int>(month,b.month);;
      return sparksee::utils::descending<std::string>(continent,b.continent);
    }
} Key;

typedef struct value_{

  int message_count;
  int likes_count;
} Value;

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Key &key, const Value &value) {
    gdb::Value val;
    ptree::ptree pt;

    pt.put<std::string>("Continent.name", key.continent);
    pt.put<int>("messageCount", value.message_count);
    pt.put<int>("likeCount", value.likes_count);
    pt.put<int>("year", key.year);
    pt.put<int>("month", key.month);

    return pt;
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache,
                            const std::map<Key, Value> &results, 
                            int limit) {
    ptree::ptree pt;
    int count = 0;
    for (std::map<Key, Value>::const_iterator it = results.begin();
        it != results.end() && count < limit; ++it, ++count) {
      pt.push_back(std::make_pair("", Project(graph, cache, it->first, it->second)));
    };
    return pt;
}

datatypes::Buffer Execute(gdb::Session *sess, const char* tag_class, int limit) {
#ifdef VERBOSE
    printf("Bi QUERY24: %s\n", tag_class);
#endif

    BEGIN_EXCEPTION

    boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
    snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

    BEGIN_TRANSACTION;

    gdb::Value value;
    
    long long class_oid = graph->FindObject(cache->tagclass_name_t, value.SetString(sparksee::utils::to_wstring(tag_class)));
    boost::scoped_ptr<gdb::Objects> class_tags(graph->Neighbors(class_oid, cache->has_type_t, gdb::Ingoing));
    
    boost::scoped_ptr<gdb::Objects> messages(graph->Neighbors(class_tags.get(), cache->has_tag_t, gdb::Ingoing));
    
    std::map<Key, Value> results;
    boost::scoped_ptr<gdb::ObjectsIterator> message_iterator(messages->Iterator());
    while (message_iterator->HasNext()) {
      long long message_oid = message_iterator->Next();
      gdb::type_t m_type = graph->GetObjectType(message_oid);
      if(m_type == cache->post_t || m_type == cache->comment_t) {
        if(m_type == cache->post_t ) {
          graph->GetAttribute(message_oid, cache->post_creation_date_t, value);
        } else {
          graph->GetAttribute(message_oid, cache->comment_creation_date_t, value);
        }
        int year = sparksee::utils::year(value.GetTimestamp());
        int month = sparksee::utils::month(value.GetTimestamp());

        boost::scoped_ptr<gdb::Objects> country(graph->Neighbors(message_oid, cache->is_located_in_t, gdb::Outgoing));
        boost::scoped_ptr<gdb::Objects> continent(graph->Neighbors(country.get(), cache->is_part_of_t, gdb::Outgoing));
        std::string name = "NULL";
        if (continent->Count() != 0) {
          long long continent_oid = continent->Any();
          graph->GetAttribute(continent_oid, cache->place_name_t, value);
          name = sparksee::utils::to_string(value.GetString());
        }
        Value &value = results[Key(year, month, name)];
        ++value.message_count;
        value.likes_count += boost::scoped_ptr<gdb::Objects>(graph->Neighbors(message_oid, cache->likes_t, gdb::Ingoing))->Count();
      }
    }


    ptree::ptree pt = Project(*graph, *cache, results, limit);
    COMMIT_TRANSACTION

#ifdef VERBOSE
    printf("EXIT BI QUERY24: %s %lu\n", tag_class, results.size());
#endif
    char* ret = snb::utils::to_json(pt);
    return datatypes::Buffer(ret, strlen(ret));

    END_EXCEPTION
}
}
}

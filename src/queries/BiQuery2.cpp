#include "Database.h"
#include "TypeCache.h"
#include "Utils.h"
#include <utils/Utils.h>
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
#include <algorithm>
#include <boostPatch/property_tree/ptree.hpp>
#include <boostPatch/property_tree/json_parser.hpp>
#include <time.h>
#include <math.h>

namespace gdb = sparksee::gdb;
namespace snb = sparksee::snb;
namespace ptree = boost::property_tree;

namespace bi {
  namespace query2 {

    typedef struct key_ {
      std::string   country_name;
      unsigned int  month;
      std::string   gender;
      unsigned int  age_group;
      std::string   tag_name;
      bool operator<(const key_& b ) const {
        if(tag_name != b.tag_name ) return tag_name < b.tag_name;
        if(age_group != b.age_group ) return age_group < b.age_group;
        if(gender != b.gender) return gender < b.gender;
        if(month != b.month) return month < b.month;
        if(country_name != b.country_name) return country_name < b.country_name;
        return false;
      }
    } Key;

    typedef struct value_ {
      int message_count;
    } Value;

    struct Result {
      Key key;
      Value value;
    };
    bool compare_result( const Result &res_a, const Result& res_b ) {
      if (res_a.value.message_count != res_b.value.message_count)
        return res_a.value.message_count > res_b.value.message_count;
      return res_a.key < res_b.key;
    }


    static ptree::ptree Project(const gdb::Graph &graph,
        const snb::TypeCache &cache, const Result &result) {
      gdb::Value val;
      ptree::ptree pt;

      pt.put<std::string>("Group.country", result.key.country_name);
      pt.put<unsigned int>("Group.month", result.key.month );
      pt.put<std::string>("Group.gender", result.key.gender );
      pt.put<unsigned int>("Group.age_group", result.key.age_group );
      pt.put<std::string>("Group.tag_name", result.key.tag_name );
      pt.put<unsigned int>("Group.count", result.value.message_count );
      return pt;
    }


    static ptree::ptree Project(const gdb::Graph &graph,
        const snb::TypeCache &cache,
        const std::vector<Result> &results, int limit) {
      ptree::ptree pt;
      int counter = 0;
      for (std::vector<Result>::const_iterator it = results.begin();
          it != results.end() && counter < limit; ++it, ++counter) {
        pt.push_back(std::make_pair("", Project(graph, cache, *it)));
      }
      return pt;
    }


    unsigned int Compute_Age_Group( long long date, long long date_end ) {
      long long diff =  date_end - date;
      boost::posix_time::ptime time(boost::gregorian::date(1970, 1, 1),
                                boost::posix_time::milliseconds(diff));
      boost::gregorian::date pdate = time.date();
      int year = pdate.year() - 1970;
      return year / 5;
    }


    datatypes::Buffer Execute(gdb::Session *sess, long long date1, long long date2, const char* country1, const char* country2, unsigned int limit)
    {
#ifdef VERBOSE
      printf("Bi QUERY2: %lli %lli %s %s", date1, date2, country1, country2);
#endif

      BEGIN_EXCEPTION

      gdb::Graph *graph = sess->GetGraph();
      snb::TypeCache *cache = snb::TypeCache::instance(graph);
      BEGIN_TRANSACTION;

      std::map<Key,Value> groups;

      long long date_end = 1356998400000;
      int threshold = 100;

      const char* countries[2];
      countries[0] = country1;
      countries[1] = country2;

      gdb::Value value;
      for (int i = 0; i < 2; ++i) {
        gdb::oid_t country_id = graph->FindObject(cache->place_name_t, value.SetString(sparksee::utils::to_wstring(std::string(countries[i]))));
        boost::scoped_ptr<gdb::Objects> cities(graph->Neighbors(country_id, cache->is_part_of_t, gdb::Ingoing));
        boost::scoped_ptr<gdb::Objects> persons(graph->Neighbors(cities.get(), cache->is_located_in_t, gdb::Ingoing));
        boost::scoped_ptr<gdb::Objects> tmpPersons(graph->Select(cache->person_t));
        persons->Intersection(tmpPersons.get());
        boost::scoped_ptr<gdb::ObjectsIterator> iter_persons(persons->Iterator());
        while(iter_persons->HasNext()) {
          gdb::oid_t person = iter_persons->Next();
          // get gender
          graph->GetAttribute(person, cache->person_gender_t, value );
          std::string gender = sparksee::utils::to_string(value.GetString());

          // get age group,  
          graph->GetAttribute(person, cache->person_birthday_t, value); 
          long long person_birthday = sparksee::utils::parse_date(sparksee::utils::to_string((const std::wstring&)value.GetString()));
          unsigned int age_group = Compute_Age_Group( person_birthday , date_end);

          boost::scoped_ptr<gdb::Objects> posts(graph->Neighbors(person, cache->post_has_creator_t, gdb::Ingoing));
          boost::scoped_ptr<gdb::ObjectsIterator> iter_posts(posts->Iterator());
          while(iter_posts->HasNext()) {
            gdb::oid_t post = iter_posts->Next();
            // get message month
            graph->GetAttribute(post, cache->post_creation_date_t, value );
            long long creation_date = value.GetTimestamp();
            if(creation_date >= date1 && creation_date <= date2 ) {
              unsigned int month = sparksee::utils::month( creation_date );

              // For each tag, create a group
              boost::scoped_ptr<gdb::Objects> tags(graph->Neighbors(post, cache->has_tag_t, gdb::Outgoing));
              boost::scoped_ptr<gdb::ObjectsIterator> iter_tags(tags->Iterator());
              while(iter_tags->HasNext()) {
                gdb::oid_t tag = iter_tags->Next();
                graph->GetAttribute(tag, cache->tag_name_t, value );

                Key key;
                key.country_name = countries[i];
                key.month = month;
                key.gender = gender;
                key.age_group = age_group;
                key.tag_name = sparksee::utils::to_string(value.GetString());

                std::map<Key,Value>::iterator it = groups.find(key);
                if(it == groups.end()) {
                  Value value;
                  value.message_count = 0;
                  it = groups.insert(std::pair<Key,Value>(key, value)).first;
                }
                it->second.message_count +=1;
              }
            }
          }

          boost::scoped_ptr<gdb::Objects> comments(graph->Neighbors(person, cache->comment_has_creator_t, gdb::Ingoing));
          boost::scoped_ptr<gdb::ObjectsIterator> iter_comments(comments->Iterator());
          while(iter_comments->HasNext()) {
            gdb::oid_t comment = iter_comments->Next();
            // get message month
            graph->GetAttribute(comment, cache->comment_creation_date_t, value );
            long long creation_date = value.GetTimestamp();
            if(creation_date >= date1 && creation_date <= date2 ) {
              unsigned int month = sparksee::utils::month( creation_date );

              // For each tag, create a group
              boost::scoped_ptr<gdb::Objects> tags(graph->Neighbors(comment, cache->has_tag_t, gdb::Outgoing));
              boost::scoped_ptr<gdb::ObjectsIterator> iter_tags(tags->Iterator());
              while(iter_tags->HasNext()) {
                gdb::oid_t tag = iter_tags->Next();
                // Create a group for each tag

                graph->GetAttribute(tag, cache->tag_name_t, value );

                Key key;
                key.country_name = countries[i];
                key.month = month;
                key.gender = gender;
                key.age_group = age_group;
                key.tag_name = sparksee::utils::to_string(value.GetString());

                std::map<Key,Value>::iterator it = groups.find(key);
                if(it == groups.end()) {
                  Value value;
                  value.message_count = 0;
                  it = groups.insert(std::pair<Key,Value>(key, value)).first;
                }
                it->second.message_count +=1;
              }
            }
          }
        }

      }

      std::vector<Result> intermediate_result;
      for( std::map<Key,Value>::iterator it = groups.begin(); it != groups.end(); ++it ) {
        Result result;
        result.key = it->first;
        result.value = it->second;
        if(result.value.message_count > threshold)
          intermediate_result.push_back(result);
      }
      std::sort(intermediate_result.begin(), intermediate_result.end(),
          compare_result);
      ptree::ptree pt = Project(*graph, *cache, intermediate_result, limit);
      COMMIT_TRANSACTION
      delete graph;
#ifdef VERBOSE
      printf("EXIT BI QUERY2: %lu\n", intermediate_result.size());
#endif

      char* ret = snb::utils::to_json(pt);
      return datatypes::Buffer(ret, strlen(ret));

      END_EXCEPTION
    }
  }
}

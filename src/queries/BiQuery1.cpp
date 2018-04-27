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
#include <algorithm>
#include <boostPatch/property_tree/ptree.hpp>
#include <boostPatch/property_tree/json_parser.hpp>
#include <time.h>

namespace gdb = sparksee::gdb;
namespace snb = sparksee::snb;
namespace ptree = boost::property_tree;


namespace bi {
    namespace query1 {

      typedef struct key_ {
        unsigned int year;
        bool is_reply;
        unsigned int category;
        bool operator<(const key_& b ) const {
          if(year != b.year) return year > b.year;
          if(is_reply != b.is_reply) return !is_reply;
          if(category != b.category) return category < b.category;
          return false;
        }
      } Key;

      typedef struct value_ {
        int count_messages;
        int sum_lengths;
      } Value;

      struct Result {
        Key key;
        Value value;
      };

      bool compare_result( const Result &res_a, const Result& res_b ) {
        return res_a.key  < res_b.key;
      }

      static ptree::ptree Project(const gdb::Graph &graph,
          const snb::TypeCache &cache, const Result &result, int total_messages) {
        gdb::Value val;
        ptree::ptree pt;

        pt.put<long long>("Group.year", result.key.year);
        pt.put<bool>("Group.isReply", result.key.is_reply);
        pt.put<int>("Group.category", result.key.category);
        pt.put<int>("Group.count",result.value.count_messages);
        pt.put<int>("Group.avg",result.value.sum_lengths / result.value.count_messages );
        pt.put<int>("Group.sum",result.value.sum_lengths);
        pt.put<float>("Group.per",result.value.count_messages / (float)total_messages );

        return pt;
      }

      static ptree::ptree Project(const gdb::Graph &graph,
          const snb::TypeCache &cache,
          const std::vector<Result> &results, int total_messages) {
        ptree::ptree pt;
        for (std::vector<Result>::const_iterator it = results.begin();
            it != results.end() ; ++it) {
          pt.push_back(std::make_pair("", Project(graph, cache, *it, total_messages)));
        }
        return pt;
      }


      int GetCategory(unsigned long length) {
        if( length < 40 ) return 0;
        if( length < 80 ) return 1;
        if( length < 160 ) return 2;
        return 3;
      }

      datatypes::Buffer Execute(gdb::Session *sess, long long date)
      {
#ifdef VERBOSE
        printf("Bi QUERY1: %lli\n", date);
#endif

        BEGIN_EXCEPTION

          gdb::Graph *graph = sess->GetGraph();
        snb::TypeCache *cache = snb::TypeCache::instance(graph);
        BEGIN_TRANSACTION;

        unsigned int total_messages = 0;
        std::map<Key,Value> groups;
        gdb::Value value;
        gdb::Objects* posts = graph->Select(cache->post_creation_date_t, gdb::LessThan, value.SetTimestamp(date));
        total_messages+=posts->Count();
        gdb::ObjectsIterator* iter_posts = posts->Iterator();
        while(iter_posts->HasNext()){
          Key key;
          gdb::oid_t post = iter_posts->Next();
          graph->GetAttribute(post, cache->post_creation_date_t, value);
          key.year = sparksee::utils::year(value.GetTimestamp()); 
          graph->GetAttribute(post, cache->post_length_t, value);
          unsigned int post_length = value.GetInteger();
          if( post_length > 0 ) {
            key.category = GetCategory(post_length);
            key.is_reply = false;
            std::map<Key,Value>::iterator it = groups.find(key);
            if( it == groups.end() ) {
              Value value;
              value.count_messages = 0;
              value.sum_lengths = 0;
              it = groups.insert(std::pair<Key,Value>(key,value)).first;
            }
            it->second.count_messages++;
            it->second.sum_lengths += post_length;
          }
        }
        delete iter_posts;
        delete posts;

        gdb::Objects* comments = graph->Select(cache->comment_creation_date_t, gdb::LessThan, value.SetTimestamp(date));
        total_messages+=comments->Count();
        gdb::ObjectsIterator* iter_comments = comments->Iterator();
        while(iter_comments->HasNext()){
          Key key;
          gdb::oid_t comment = iter_comments->Next();
          graph->GetAttribute(comment, cache->comment_creation_date_t, value);
          key.year = sparksee::utils::year(value.GetTimestamp()); 
          graph->GetAttribute(comment, cache->comment_length_t, value);
          unsigned int comment_length = value.GetInteger();
          if(comment_length > 0) {
            key.category = GetCategory(comment_length);
            key.is_reply = true;
            std::map<Key,Value>::iterator it = groups.find(key);
            if( it == groups.end() ) {
              Value value;
              value.count_messages = 0;
              value.sum_lengths = 0;
              it = groups.insert(std::pair<Key,Value>(key,value)).first;
            }
            it->second.count_messages++;
            it->second.sum_lengths += comment_length;
          }
        }
        delete iter_comments;
        delete comments;

        std::vector<Result> intermediate_result;
        for(std::map<Key,Value>::iterator it = groups.begin(); it != groups.end(); ++it) {
          Result result;
          result.key = (*it).first;
          result.value = (*it).second;
          intermediate_result.push_back(result);
        }
        std::sort(intermediate_result.begin(), intermediate_result.end(),
            compare_result);
        ptree::ptree pt = Project(*graph, *cache, intermediate_result, total_messages);
        COMMIT_TRANSACTION
          delete graph;
#ifdef VERBOSE
        printf("EXIT BI QUERY1: %lli %lu\n", date, intermediate_result.size());
#endif
        char* ret = snb::utils::to_json(pt);
        return datatypes::Buffer(ret, strlen(ret));
        END_EXCEPTION
      }
    }
}


#include "snbInteractive.h"
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
#include <algorithm>
#include <stdio.h>
#include <boostPatch/property_tree/ptree.hpp>
#include <boostPatch/property_tree/json_parser.hpp>

namespace gdb = sparksee::gdb;
namespace snb = sparksee::snb;
namespace ptree = boost::property_tree;

namespace interactive {
  namespace short7{

    struct Result {
      long long creation_date;
      long long comment_id;
      gdb::oid_t comment_oid;
      long long author_id;
      gdb::oid_t author_oid;
      bool known;
    };

    bool compare_result(const Result &res_a, const Result &res_b) {
      if( res_a.creation_date != res_b.creation_date ) {
        return res_a.creation_date > res_b.creation_date;
      }
      return res_a.author_id > res_b.author_id;
    }

    static ptree::ptree Project(const gdb::Graph &graph,
        const snb::TypeCache &cache, const Result &result) {
      ptree::ptree pt;
      gdb::Graph& nc_graph = const_cast<gdb::Graph&>(graph);
      gdb::Value val;
      pt.put<long long>("Comment.id", result.comment_id);
      nc_graph.GetAttribute(result.comment_oid, cache.comment_content_t, val);
      pt.put<std::string>("Comment.content", sparksee::utils::to_string(val.GetString()));
      pt.put<long long>("Comment.creationDate", result.creation_date);
      nc_graph.GetAttribute(result.author_oid, cache.person_id_t, val);
      pt.put<long long>("Person.id", val.GetLong());
      nc_graph.GetAttribute(result.author_oid, cache.person_first_name_t, val);
      pt.put<std::string>("Person.firstName", sparksee::utils::to_string(val.GetString()));
      nc_graph.GetAttribute(result.author_oid, cache.person_last_name_t, val);
      pt.put<std::string>("Person.lastName", sparksee::utils::to_string(val.GetString()));
      pt.put<bool>("Known", result.known);
      return pt;
    }

    static ptree::ptree Project(const gdb::Graph &graph,
        const snb::TypeCache &cache,
        const std::vector<Result> &results) {
      ptree::ptree pt;
      for (std::vector<Result>::const_iterator it = results.begin();
          it != results.end() ; ++it) {
        pt.push_back(std::make_pair("", Project(graph, cache, *it)));
      }
      return pt;
    }

    datatypes::Buffer Execute(gdb::Session *sess, long long message_id) {
#ifdef VERBOSE
      printf("SHORT QUERY7:\n");
#endif

      BEGIN_EXCEPTION
        gdb::Graph *graph = sess->GetGraph();
        snb::TypeCache *cache = snb::TypeCache::instance(graph);
        std::vector<Result> intermediate_result;
        gdb::Value val;
        val.SetLong(message_id);
        BEGIN_TRANSACTION
        gdb::oid_t message_oid = graph->FindObject(cache->post_id_t, val);
        if(message_oid == gdb::Objects::InvalidOID) {
          message_oid = graph->FindObject(cache->comment_id_t,val);
        }
        gdb::Objects* creators = graph->Neighbors(message_oid, cache->post_has_creator_t, gdb::Outgoing);
        if(creators->Count()==0) {
          delete creators;
          creators = graph->Neighbors(message_oid, cache->comment_has_creator_t, gdb::Outgoing);
        }
        gdb::oid_t input_creator_oid = creators->Any();
        delete creators;
        gdb::Objects* replies = graph->Neighbors(message_oid, cache->reply_of_t, gdb::Ingoing);
        gdb::ObjectsIterator* iter_replies = replies->Iterator();
        while(iter_replies->HasNext()) {
          gdb::oid_t reply_oid = iter_replies->Next();
          Result res;
          graph->GetAttribute(reply_oid, cache->comment_creation_date_t, val);
          res.creation_date = val.GetTimestamp();
          res.comment_oid = reply_oid;
          graph->GetAttribute(reply_oid, cache->comment_id_t, val);
          res.comment_id = val.GetLong();
          gdb::Objects* creators = graph->Neighbors(reply_oid, cache->comment_has_creator_t, gdb::Outgoing);
          gdb::oid_t creator_oid = creators->Any();
          delete creators;
          graph->GetAttribute(creator_oid, cache->person_id_t, val);
          res.author_oid = creator_oid;
          res.author_id = val.GetLong();
          gdb::Objects* friends = graph->Neighbors(creator_oid, cache->knows_t, gdb::Outgoing);
          res.known =  friends->Exists(input_creator_oid); 
          delete friends;
          intermediate_result.push_back(res);
        }
        delete iter_replies;
        delete replies;
        std::sort(intermediate_result.begin(), intermediate_result.end(),
            compare_result);
        ptree::ptree pt = Project(*graph, *cache, intermediate_result);
        COMMIT_TRANSACTION
        delete graph;
#ifdef VERBOSE
        printf("EXIT SHORT QUERY7:\n");
#endif
        char* ret = snb::utils::to_json(pt);
        return datatypes::Buffer(ret,strlen(ret));
        END_EXCEPTION
    }
  }
}

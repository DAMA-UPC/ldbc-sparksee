
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
    namespace short6{

        struct Result {
            long long forum_id;
            std::string forum_title;
            long long moderator_id;
            std::string moderator_first_name;
            std::string moderator_last_name;
        };

        static ptree::ptree Project(const gdb::Graph &graph,
                const snb::TypeCache &cache, const Result &result) {
            ptree::ptree pt;
            pt.put<long long>("Forum.id", result.forum_id);
            pt.put<std::string>("Forum.title", result.forum_title);
            pt.put<long long>("Person.id", result.moderator_id);
            pt.put<std::string>("Person.firstName", result.moderator_first_name);
            pt.put<std::string>("Person.lastName", result.moderator_last_name);
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

        datatypes::Buffer Execute(gdb::Session *sess, long long message_id) {
#ifdef VERBOSE
            printf("SHORT QUERY6:\n");
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
                gdb::type_t message_type = cache->comment_t;
                while(message_type != cache->post_t){
                    gdb::Objects* replies = graph->Neighbors(message_oid, cache->reply_of_t, gdb::Outgoing);
                    message_oid = replies->Any();
                    message_type = graph->GetObjectType(message_oid);
                    delete replies;
                } 
            }

            gdb::Objects* forums = graph->Neighbors(message_oid, cache->container_of_t, gdb::Ingoing);
            gdb::oid_t forum_oid = forums->Any();
            delete forums;

            Result res;
            graph->GetAttribute(forum_oid, cache->forum_id_t, val);
            res.forum_id = val.GetLong();
            graph->GetAttribute(forum_oid, cache->forum_title_t, val);
            res.forum_title = sparksee::utils::to_string(val.GetString());

            gdb::Objects* moderators = graph->Neighbors(forum_oid, cache->has_moderator_t, gdb::Outgoing);
            gdb::oid_t moderator_oid = moderators->Any();
            delete moderators;

            graph->GetAttribute(moderator_oid, cache->person_id_t, val);
            res.moderator_id = val.GetLong();
            graph->GetAttribute(moderator_oid, cache->person_first_name_t, val);
            res.moderator_first_name = sparksee::utils::to_string(val.GetString());
            graph->GetAttribute(moderator_oid, cache->person_last_name_t, val);
            res.moderator_last_name = sparksee::utils::to_string(val.GetString());
            intermediate_result.push_back(res);

            ptree::ptree pt = Project(*graph, *cache, intermediate_result, 1);
            COMMIT_TRANSACTION
            delete graph;
#ifdef VERBOSE
            printf("EXIT SHORT QUERY6:\n");
#endif
            char* ret = snb::utils::to_json(pt);
            return datatypes::Buffer(ret, strlen(ret));
            END_EXCEPTION
        }
    }
}

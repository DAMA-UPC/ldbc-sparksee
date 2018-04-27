
#include "snbInteractive.h"
#include "Database.h"
#include "TypeCache.h"
#include "Utils.h"
#include <utils/Utils.h>
#include <utils/GroupBy.h>

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
    namespace query7 {

        struct Result {
            long long creation_date;
            long long person_id;
            long long message_id;
            gdb::oid_t person_oid;
            gdb::oid_t message_oid;
            gdb::type_t type;
            long long latency;
        };

        bool compare_result(const Result &res_a, const Result &res_b) {
            if( res_a.creation_date != res_b.creation_date ) {
                return res_a.creation_date > res_b.creation_date;
            }
            return res_a.person_id < res_b.person_id;
        }

        static ptree::ptree Project(const gdb::Graph &graph,
                const snb::TypeCache &cache, const Result &result, gdb::Objects* friends) {
            gdb::Value val;
            ptree::ptree pt;
            gdb::Graph& nc_graph = const_cast<gdb::Graph&>(graph);
            pt.put<long long>("Person.id", result.person_id);
            nc_graph.GetAttribute(result.person_oid, cache.person_first_name_t, val);
            pt.put<std::string>("Person.firstName", sparksee::utils::to_string(val.GetString()));
            nc_graph.GetAttribute(result.person_oid, cache.person_last_name_t, val);
            pt.put<std::string>("Person.lastName", sparksee::utils::to_string(val.GetString()));
            pt.put<long long>("Like.creationDate", result.creation_date );
            pt.put<long long>("Message.id", result.message_id );
            gdb::attr_t content_attr = result.type == cache.post_t ? cache.post_content_t : cache.comment_content_t;;
            nc_graph.GetAttribute(result.message_oid, content_attr, val);
            if( val.IsNull() ) {
                nc_graph.GetAttribute(result.message_oid, cache.post_image_file_t, val);
            }
            pt.put<std::string>("Message.content", sparksee::utils::to_string(val.GetString()));
            pt.put<long long>("latency", result.latency);
            pt.put<bool>("isNew", !friends->Exists(result.person_oid));
            return pt;
        }

        static ptree::ptree Project(const gdb::Graph &graph,
                const snb::TypeCache &cache,
                const std::vector<Result> &results, gdb::Objects* friends, int limit) {
            ptree::ptree pt;
            int counter = 0;
            std::set<gdb::oid_t> visited;
            for (std::vector<Result>::const_iterator it = results.begin();
                    it != results.end() && counter < limit; ++it) {
                if( visited.find(it->person_oid) == visited.end() ) {
                    counter++;
                    visited.insert(it->person_oid);
                    pt.push_back(std::make_pair("", Project(graph, cache, *it, friends)));
                }
            }
            return pt;
        }

        datatypes::Buffer Execute(gdb::Session *sess , long long person_id,
                unsigned int limit) {
#ifdef VERBOSE
            printf("QUERY7: %lli %u\n", person_id, limit);
            timeval start;
            gettimeofday(&start,NULL);
#endif
            BEGIN_EXCEPTION
            gdb::Graph *graph = sess->GetGraph();
            snb::TypeCache *cache = snb::TypeCache::instance(graph);
            std::vector<Result> intermediate_result;
            gdb::Value val;
            val.SetLong(person_id);
            BEGIN_TRANSACTION

            timeval startCore;
            gettimeofday(&startCore,NULL);

            gdb::oid_t person_oid = graph->FindObject(cache->person_id_t, val);
            gdb::Objects* posts = graph->Neighbors(person_oid, cache->post_has_creator_t, gdb::Ingoing);
            gdb::Objects* comments = graph->Neighbors(person_oid, cache->comment_has_creator_t, gdb::Ingoing);
            gdb::Objects* messages = gdb::Objects::CombineUnion(posts,comments);
            delete posts;
            delete comments;
            gdb::Objects* likes = graph->Explode(messages, cache->likes_t, gdb::Ingoing);
            gdb::ObjectsIterator* iter_likes = likes->Iterator();
            while(iter_likes->HasNext()) {
                gdb::oid_t like = iter_likes->Next();
                graph->GetAttribute(like, cache->likes_creation_date_t,val);
                long long creation_date = val.GetTimestamp();
                gdb::EdgeData* e_data = graph->GetEdgeData(like);
                gdb::oid_t message_oid = e_data->GetHead();
                gdb::type_t message_type = graph->GetObjectType(message_oid);
                long long message_id = 0;
                long long message_creation_date = 0;
                if( message_type == cache->post_t ) {
                    graph->GetAttribute( message_oid, cache->post_id_t, val); 
                    message_id = val.GetLong();
                    graph->GetAttribute( message_oid, cache->post_creation_date_t, val);
                    message_creation_date = val.GetTimestamp();
                } else {
                    graph->GetAttribute( message_oid, cache->comment_id_t, val); 
                    message_id = val.GetLong();
                    graph->GetAttribute( message_oid, cache->comment_creation_date_t, val);
                    message_creation_date = val.GetTimestamp();
                }
                graph->GetAttribute(e_data->GetTail(), cache->person_id_t, val);
                long long creator_id = val.GetLong();
                long long latency = (creation_date - message_creation_date) / (1000L*60L);
                Result res = { creation_date, creator_id, message_id, e_data->GetTail(), message_oid, message_type, latency };
                intermediate_result.push_back(res);
            }
            delete iter_likes;
            delete likes;
            delete messages;

            timeval endCore;
            gettimeofday(&endCore,NULL);

            std::sort(intermediate_result.begin(), intermediate_result.end(),
                    compare_result);
            gdb::Objects* friends = graph->Neighbors(person_oid, cache->knows_t, gdb::Outgoing);
            ptree::ptree pt = Project(*graph, *cache, intermediate_result, friends, limit);
            COMMIT_TRANSACTION
            delete friends;
            delete graph;
#ifdef VERBOSE
            printf("EXIT QUERY7: %lli %u\n", person_id, limit);
            /*
            timeval end;
            gettimeofday(&end,NULL);
            unsigned long executionTime = (end.tv_sec - start.tv_sec)*1000 + (end.tv_usec - start.tv_usec)/1000;
            unsigned long executionTimeCore = (endCore.tv_sec - startCore.tv_sec)*1000 + (endCore.tv_usec - startCore.tv_usec)/1000;
            printf("QUERY7:EXECUTION_TIME: %lu %lu\n",executionTime, executionTimeCore);
            */
#endif
            char* res = snb::utils::to_json(pt);
            return datatypes::Buffer(res, strlen(res));
            END_EXCEPTION
        }
    }
}

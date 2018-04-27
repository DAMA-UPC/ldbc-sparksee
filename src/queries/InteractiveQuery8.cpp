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
    namespace query8 {

        struct Result {
            long long creation_date;
            long long comment_id;
            gdb::oid_t comment_oid;
        };

        bool compare_result(const Result &res_a, const Result &res_b) {
            if( res_a.creation_date != res_b.creation_date ) {
                return res_a.creation_date > res_b.creation_date;
            }
            return res_a.comment_id < res_b.comment_id;
        }

        static ptree::ptree Project(const gdb::Graph &graph,
                const snb::TypeCache &cache, const Result &result) {
            gdb::Value val;
            ptree::ptree pt;
            gdb::Graph& nc_graph = const_cast<gdb::Graph&>(graph);
            gdb::Objects* creator = nc_graph.Neighbors(result.comment_oid, cache.comment_has_creator_t, gdb::Outgoing );
            gdb::oid_t person_oid = creator->Any();
            delete creator;
            nc_graph.GetAttribute(person_oid, cache.person_id_t, val);
            pt.put<long long>("Person.id", val.GetLong());
            nc_graph.GetAttribute(person_oid, cache.person_first_name_t, val);
            pt.put<std::string>("Person.firstName", sparksee::utils::to_string(val.GetString()));
            nc_graph.GetAttribute(person_oid, cache.person_last_name_t, val);
            pt.put<std::string>("Person.lastName", sparksee::utils::to_string(val.GetString()));
            pt.put<long long>("Comment.creationDate", result.creation_date);
            pt.put<long long>("Comment.id", result.comment_id);
            nc_graph.GetAttribute(result.comment_oid, cache.comment_content_t, val);
            pt.put<std::string>("Comment.content", sparksee::utils::to_string(val.GetString()));
            
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

        datatypes::Buffer Execute(gdb::Session *sess, long long person_id,
                unsigned int limit) {
#ifdef VERBOSE
            printf("QUERY8: %lli %u\n", person_id, limit);
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
            gdb::Objects* replies = graph->Neighbors(messages, cache->reply_of_t, gdb::Ingoing);
            gdb::ObjectsIterator* iter_replies = replies->Iterator();
            while(iter_replies->HasNext()) {
                gdb::oid_t reply = iter_replies->Next();
                graph->GetAttribute(reply, cache->comment_creation_date_t, val);
                long long creation_date = val.GetTimestamp();
                graph->GetAttribute(reply, cache->comment_id_t, val);
                long long comment_id = val.GetLong();
                Result res = {creation_date, comment_id, reply};
                intermediate_result.push_back(res);
            }
            delete iter_replies;
            delete replies;
            delete messages;

            timeval endCore;
            gettimeofday(&endCore,NULL);

            std::sort(intermediate_result.begin(), intermediate_result.end(),
                    compare_result);
            ptree::ptree pt = Project(*graph, *cache, intermediate_result, limit);
            COMMIT_TRANSACTION
            delete graph;
#ifdef VERBOSE
            printf("EXIT QUERY8: %lli %u\n", person_id, limit);
            /*timeval end;
            gettimeofday(&end,NULL);
            unsigned long executionTime = (end.tv_sec - start.tv_sec)*1000 + (end.tv_usec - start.tv_usec)/1000;
            unsigned long executionTimeCore = (endCore.tv_sec - startCore.tv_sec)*1000 + (endCore.tv_usec - startCore.tv_usec)/1000;
            printf("QUERY8:EXECUTION_TIME: %lu %lu\n",executionTime, executionTimeCore);
            */
#endif
            char* res = snb::utils::to_json(pt);

            return datatypes::Buffer(res, strlen(res));
            END_EXCEPTION
        }
    }
}

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
#include <time.h>

namespace gdb = sparksee::gdb;
namespace snb = sparksee::snb;
namespace ptree = boost::property_tree;

namespace interactive {
    namespace query5 {

        struct Result {
            long count;
            long long forum_id;
            gdb::oid_t forum_oid;
        };

        bool compare_result(const Result &res_a, const Result &res_b) {
            if (res_a.count != res_b.count )  
                return res_a.count > res_b.count;
            return res_a.forum_id < res_b.forum_id;
        }

        static ptree::ptree Project(const gdb::Graph &graph,
                const snb::TypeCache &cache, const Result &result) {
            gdb::Graph &nc_graph = const_cast<gdb::Graph &>(graph);
            gdb::Value val;
            ptree::ptree pt;
            nc_graph.GetAttribute( result.forum_oid, cache.forum_title_t, val);
            pt.put<std::string>("Forum.title", sparksee::utils::to_string(val.GetString()));
            pt.put<long long>("count", result.count );
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
                long long min_date, unsigned int limit) {

#ifdef VERBOSE
            printf("QUERY5: %lli %llu %u\n", person_id, min_date, limit);
            timeval start;
            gettimeofday(&start,NULL);
#endif
            BEGIN_EXCEPTION
            gdb::Graph *graph = sess->GetGraph();
            snb::TypeCache *cache = snb::TypeCache::instance(graph);
            std::vector<Result> intermediate_result;

            gdb::Value min_date_val;
            min_date_val.SetTimestamp(min_date);
            gdb::Value val;
            val.SetLong(person_id);
            BEGIN_TRANSACTION

            timeval startCore;
            gettimeofday(&startCore,NULL);

            gdb::oid_t person_oid = graph->FindObject( cache->person_id_t, val );
            gdb::Objects* friends = sparksee::utils::k_hop( *sess, *graph, person_oid, cache->knows_t, gdb::Outgoing, 2, false);
            gdb::Objects* memberships = graph->Explode( friends, cache->has_member_with_posts_t, gdb::Ingoing );
            gdb::Objects* filter_memberships = graph->Select(cache->has_member_with_posts_join_date_t, gdb::GreaterThan, min_date_val, memberships); 
            gdb::Objects* forums = graph->Tails(filter_memberships);
            delete filter_memberships;
            gdb::ObjectsIterator* iter_forums = forums->Iterator();
            while(iter_forums->HasNext()){
                gdb::oid_t forum_oid = iter_forums->Next();
                int counter = 0;
                gdb::Objects* forum_posts = graph->Neighbors( forum_oid, cache->container_of_t, gdb::Outgoing );
                gdb::ObjectsIterator* iter_posts = forum_posts->Iterator();
                while(iter_posts->HasNext()) {
                    gdb::oid_t post_oid = iter_posts->Next();
                    gdb::Objects* post_creator = graph->Neighbors(post_oid, cache->post_has_creator_t, gdb::Outgoing);
                    gdb::oid_t creator_oid =  post_creator->Any();
                    if( friends->Exists(creator_oid) ) {
                        gdb::oid_t membership_edge = graph->FindEdge(cache->has_member_t, forum_oid, creator_oid);
                        if( membership_edge != gdb::Objects::InvalidOID ) {
                            graph->GetAttribute(membership_edge,cache->has_member_join_date_t, val);
                            if( val.GetTimestamp() >  min_date ){
                                counter++;
                            }
                        }
                    }
                    delete post_creator;
                }
                delete iter_posts;
                delete forum_posts;
                graph->GetAttribute(forum_oid, cache->forum_id_t, val);
                Result res = {counter,val.GetLong(),forum_oid}; 
                intermediate_result.push_back(res);
                graph->GetAttribute(forum_oid,cache->forum_id_t, val);
            }
            delete iter_forums;
            if( intermediate_result.size() < limit ) {
                gdb::Objects* all_memberships = graph->Explode( friends, cache->has_member_t, gdb::Ingoing );
                gdb::Objects* filter_memberships = graph->Select(cache->has_member_join_date_t, gdb::GreaterThan, min_date_val, all_memberships); 
                gdb::Objects* all_forums = graph->Tails(filter_memberships);
                delete filter_memberships;
                delete all_memberships;
                all_forums->Difference(forums);
                gdb::ObjectsIterator* iter_forums = all_forums->Iterator();
                while(iter_forums->HasNext()){
                    gdb::oid_t forum_oid = iter_forums->Next();
                    graph->GetAttribute(forum_oid, cache->forum_id_t, val);
                    Result res = {0,val.GetLong(),forum_oid}; 
                    intermediate_result.push_back(res);
                }
                delete iter_forums;
                delete all_forums;
            }
            delete forums;
            delete memberships;
            delete friends;

            timeval endCore;
            gettimeofday(&endCore,NULL);

            std::sort(intermediate_result.begin(), intermediate_result.end(), compare_result);
            ptree::ptree pt = Project(*graph, *cache, intermediate_result, limit);
            COMMIT_TRANSACTION
            delete graph;
#ifdef VERBOSE
            printf("EXIT QUERY5: %lli %llu %u\n", person_id, min_date, limit);
            /*timeval end;
            gettimeofday(&end,NULL);
            unsigned long executionTime = (end.tv_sec - start.tv_sec)*1000 + (end.tv_usec - start.tv_usec)/1000;
            unsigned long executionTimeCore = (endCore.tv_sec - startCore.tv_sec)*1000 + (endCore.tv_usec - startCore.tv_usec)/1000;
            printf("QUERY5:EXECUTION_TIME: %lu %lu\n",executionTime, executionTimeCore);
            */
#endif
            char* res = snb::utils::to_json(pt);
            return datatypes::Buffer(res, strlen(res));
            END_EXCEPTION
        }
    }
}

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
    namespace query10 {


        struct Result {
            int similarity;
            long long person_id;
            gdb::oid_t person_oid;
        };

        bool compare_result(const Result &res_a, const Result &res_b) {
            if( res_a.similarity != res_b.similarity ) {
                return res_a.similarity > res_b.similarity;
            }
            return res_a.person_id < res_b.person_id;
        }

        static ptree::ptree Project(const gdb::Graph &graph,
                const snb::TypeCache &cache, const Result &result) {
            gdb::Value val;
            ptree::ptree pt;
            gdb::Graph& nc_graph = const_cast<gdb::Graph&>(graph);
            pt.put<long long>("Person.id", result.person_id);
            nc_graph.GetAttribute(result.person_oid, cache.person_first_name_t, val);
            pt.put<std::string>("Person.firstName", sparksee::utils::to_string(val.GetString()));
            nc_graph.GetAttribute(result.person_oid, cache.person_last_name_t, val);
            pt.put<std::string>("Person.lastName", sparksee::utils::to_string(val.GetString()));
            pt.put<long long>("similarity", result.similarity );
            nc_graph.GetAttribute(result.person_oid, cache.person_gender_t, val);
            pt.put<std::string>("Person.gender", sparksee::utils::to_string(val.GetString()));
            gdb::Objects* places = nc_graph.Neighbors(result.person_oid, cache.is_located_in_t, gdb::Outgoing);
            gdb::oid_t place_oid = places->Any();
            nc_graph.GetAttribute(place_oid, cache.place_name_t, val);
            pt.put<std::string>("Place.name", sparksee::utils::to_string(val.GetString()));
            delete places;
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
                unsigned int month, unsigned int limit) {
#ifdef VERBOSE
            printf("QUERY10: %lli %u %u\n", person_id, month, limit);
            timeval start;
            gettimeofday(&start,NULL);
#endif

            BEGIN_EXCEPTION

            gdb::Graph *graph = sess->GetGraph();
            snb::TypeCache *cache = snb::TypeCache::instance(graph);
            std::vector<Result> intermediate_result;
            gdb::Value val;
            val.SetLong(person_id);
            unsigned int next_month = ((month % 12)+1);
            BEGIN_TRANSACTION

            timeval startCore;
            gettimeofday(&startCore,NULL);

            gdb::oid_t person_oid = graph->FindObject(cache->person_id_t, val);
            gdb::Objects* person_tags = graph->Neighbors(person_oid, cache->has_interest_t, gdb::Outgoing);
            //clock_t start = clock();
            gdb::Objects* friends = graph->Neighbors(person_oid, cache->knows_t, gdb::Outgoing);
            gdb::Objects* friends_up_to_two = sparksee::utils::k_hop(*sess, *graph, person_oid, cache->knows_t, gdb::Outgoing, 2, false);
            gdb::Objects* two_hop_friends = gdb::Objects::CombineDifference(friends_up_to_two, friends);
            //printf("Time to friends %f\n",(clock()-start)/((float)CLOCKS_PER_SEC));
            gdb::ObjectsIterator* iter_friends = two_hop_friends->Iterator();
            //start = clock();
            while(iter_friends->HasNext()) {
                gdb::oid_t friend_oid = iter_friends->Next();
                graph->GetAttribute(friend_oid, cache->person_birthday_t, val);
                std::string birthday = sparksee::utils::to_string(val.GetString());
                unsigned int friend_month = sparksee::utils::month(birthday);
                unsigned int friend_day = sparksee::utils::day(birthday);
                if( (friend_day >= 21 && month == friend_month) ||
                    (friend_day < 22 && next_month == friend_month)) {
                    int similarity = 0;
                    gdb::Objects* friend_posts = graph->Neighbors(friend_oid, cache->post_has_creator_t, gdb::Ingoing);
                    gdb::ObjectsIterator* iter_posts = friend_posts->Iterator();
                    while(iter_posts->HasNext()) {
                        gdb::oid_t post_oid = iter_posts->Next();
                        gdb::Objects* post_tags = graph->Neighbors(post_oid, cache->has_tag_t, gdb::Outgoing);
                        post_tags->Intersection(person_tags);
                        similarity += post_tags->Count() > 0 ? 1 : -1;
                        delete post_tags;
                    }

                    delete iter_posts;
                    delete friend_posts;
                    graph->GetAttribute(friend_oid, cache->person_id_t, val);
                    Result res; 
                    res.similarity = similarity;
                    res.person_id = val.GetLong();
                    res.person_oid = friend_oid;
                    intermediate_result.push_back(res);
                }
            }
//            printf("Time to similarity %f\n",(clock()-start)/((float)CLOCKS_PER_SEC));
            delete iter_friends;
            delete two_hop_friends;
            delete friends_up_to_two;
            delete friends;
            delete person_tags;

            timeval endCore;
            gettimeofday(&endCore,NULL);

            std::sort(intermediate_result.begin(), intermediate_result.end(),
                    compare_result);
            ptree::ptree pt = Project(*graph, *cache, intermediate_result, limit);
            COMMIT_TRANSACTION
            delete graph;
#ifdef VERBOSE
            printf("EXIT QUERY10: %lli %u %u\n", person_id, month, limit);
            /*
            timeval end;
            gettimeofday(&end,NULL);
            unsigned long executionTime = (end.tv_sec - start.tv_sec)*1000 + (end.tv_usec - start.tv_usec)/1000;
            unsigned long executionTimeCore = (endCore.tv_sec - startCore.tv_sec)*1000 + (endCore.tv_usec - startCore.tv_usec)/1000;
            printf("QUERY10:EXECUTION_TIME: %lu %lu\n",executionTime, executionTimeCore);
            */
#endif
            char* res = snb::utils::to_json(pt);
            return datatypes::Buffer(res, strlen(res));
            END_EXCEPTION
        }
    }
}

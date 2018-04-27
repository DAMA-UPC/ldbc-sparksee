
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
    namespace query4 {

        struct Result {
            std::string tag_name;
            int     count;
        };

        bool compare_result(const Result &res_a, const Result &res_b) {
            if (res_a.count != res_b.count )  
                return res_a.count > res_b.count;
            return res_a.tag_name < res_b.tag_name;
        }

        static ptree::ptree Project(const gdb::Graph &graph,
                const snb::TypeCache &cache, const Result &result) {
            gdb::Value val;
            ptree::ptree pt;
            pt.put<std::string>("Tag.name", result.tag_name);
            pt.put<int>("count", result.count );
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
                long long date, unsigned int days,
                unsigned int limit) {
#ifdef VERBOSE
            printf("QUERY4: %lli %llu %u %u\n", person_id, date, days, limit);
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

            gdb::oid_t person_oid = graph->FindObject( cache->person_id_t, val );  
            gdb::Value start_date;
            start_date.SetTimestamp(date);
            gdb::Value end_date;
            end_date.SetTimestamp(date + days*24L*3600L*1000L);
            gdb::Objects* tags_before = sess->NewObjects(); 
            std::map<gdb::oid_t, int> tag_count;
            gdb::Objects* friends = graph->Neighbors( person_oid, cache->knows_t, gdb::Outgoing );
            gdb::ObjectsIterator* iter_friends = friends->Iterator();
            while(iter_friends->HasNext()) {
                gdb::oid_t friend_oid = iter_friends->Next();
                gdb::Objects* friend_posts = graph->Neighbors(friend_oid, cache->post_has_creator_t, gdb::Ingoing);
                gdb::ObjectsIterator* iter_posts = friend_posts->Iterator();
                while( iter_posts->HasNext() ) {
                    gdb::oid_t post_oid = iter_posts->Next();
                    graph->GetAttribute(post_oid, cache->post_creation_date_t, val );
                    long long creation_date = val.GetTimestamp();
                    if( creation_date < start_date.GetTimestamp() ) {
                        gdb::Objects* tags = graph->Neighbors(post_oid, cache->has_tag_t, gdb::Outgoing );
                        tags_before->Union(tags);
                        delete tags;
                    } else if ( creation_date >= start_date.GetTimestamp() && creation_date < end_date.GetTimestamp() ) {
                        gdb::Objects* tags = graph->Neighbors(post_oid, cache->has_tag_t, gdb::Outgoing );
                        gdb::ObjectsIterator* iter_tags = tags->Iterator();
                        while(iter_tags->HasNext()) {
                            gdb::oid_t tag = iter_tags->Next();
                            if( !tags_before->Exists(tag) ) {
                                std::map<gdb::oid_t, int>::iterator it = tag_count.find(tag);
                                if( it == tag_count.end() ) {
                                    tag_count[tag] = 1;
                                } else {
                                    tag_count[tag] = it->second +1;
                                }
                            } 
                        }
                        delete iter_tags;
                        delete tags;
                    }
                }
                delete iter_posts;
                delete friend_posts;
            }
            delete iter_friends;
            delete friends;

            timeval endCore;
            gettimeofday(&endCore,NULL);

            for( std::map<gdb::oid_t, int>::iterator it = tag_count.begin(); it != tag_count.end(); ++it) {
                if( !tags_before->Exists(it->first) ) {
                    /*gdb::Objects* tag_posts = graph->Neighbors(it->first, cache->has_tag_t, gdb::Ingoing);
                    gdb::ObjectsIterator* iter_posts = tag_posts->Iterator();
                    bool found = false;
                    while(iter_posts->HasNext()) {
                        gdb::oid_t post_oid = iter_posts->Next();
                        if( graph->GetObjectType(post_oid) == cache->post_t ) {
                            graph->GetAttribute(post_oid, cache->post_creation_date_t, val );
                            if(val.GetTimestamp() < start_date.GetTimestamp()) {
                                found = true;
                                break;
                            }
                        }
                    }
                    delete iter_posts;
                    delete tag_posts;
                    if( !found ) { */
                        graph->GetAttribute(it->first, cache->tag_name_t, val); 
                        Result res = {sparksee::utils::to_string(val.GetString()), it->second};
                        intermediate_result.push_back(res);
                    //}
                }
            }
            delete tags_before;

            std::sort(intermediate_result.begin(), intermediate_result.end(), compare_result);
            ptree::ptree pt = Project(*graph, *cache, intermediate_result, limit);
            COMMIT_TRANSACTION
            delete graph;
#ifdef VERBOSE
            printf("EXIT QUERY4: %lli %llu %u %u\n", person_id, date, days, limit);
            /*timeval end;
            gettimeofday(&end,NULL);
            unsigned long executionTime = (end.tv_sec - start.tv_sec)*1000 + (end.tv_usec - start.tv_usec)/1000;
            unsigned long executionTimeCore = (endCore.tv_sec - startCore.tv_sec)*1000 + (endCore.tv_usec - startCore.tv_usec)/1000;
            printf("QUERY4:EXECUTION_TIME: %lu %lu\n",executionTime, executionTimeCore);
            */
#endif
            char* ret = snb::utils::to_json(pt);
            return datatypes::Buffer(ret, strlen(ret));
            END_EXCEPTION
        }
    }
}

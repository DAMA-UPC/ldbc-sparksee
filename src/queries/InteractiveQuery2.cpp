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
#include <queue>
#include <time.h>

namespace gdb = sparksee::gdb;
namespace snb = sparksee::snb;
namespace ptree = boost::property_tree;

namespace interactive {
    namespace query2 {

        enum MessageType {
            kPost,
            kComment,
        };

        struct Result {
            long long creation_date;
            long long message_id;
            gdb::oid_t creator_oid;
            gdb::oid_t message_oid;
            MessageType type;
        };

        bool compare_result(const Result &res_a, const Result &res_b) {
            if (res_a.creation_date != res_b.creation_date)  
                return res_a.creation_date > res_b.creation_date;
            return res_a.message_id < res_b.message_id;
        }

        class mycomparison
        {
            public:
            bool operator() (const Result& res_a, const Result& res_b) const
            {
                if (res_a.creation_date != res_b.creation_date)  
                    return res_a.creation_date > res_b.creation_date;
                return res_a.message_id < res_b.message_id;
            }
        };

        static ptree::ptree Project(const gdb::Graph &graph,
                const snb::TypeCache &cache, const Result &result) {
            gdb::Graph &nc_graph = const_cast<gdb::Graph &>(graph);
            gdb::Value val;
            ptree::ptree pt;
            nc_graph.GetAttribute( result.creator_oid, cache.person_id_t, val);
            pt.put<long long>("Person.id", val.GetLong());
            nc_graph.GetAttribute( result.creator_oid, cache.person_first_name_t, val);
            pt.put<std::string>("Person.firstName", sparksee::utils::to_string(val.GetString()));
            nc_graph.GetAttribute( result.creator_oid, cache.person_last_name_t, val);
            pt.put<std::string>("Person.lastName", sparksee::utils::to_string(val.GetString()));
            pt.put<long long>("Message.id", result.message_id );
            nc_graph.GetAttribute( result.message_oid, result.type == kPost ? cache.post_content_t : cache.comment_content_t, val);
            if( result.type == kPost && val.IsNull() ) {
                nc_graph.GetAttribute( result.message_oid, cache.post_image_file_t, val);
            }
            pt.put<std::string>("Message.content", sparksee::utils::to_string(val.GetString()));
            pt.put<long long>("Message.creationDate", result.creation_date );
            return pt;
        }

        static ptree::ptree Project(const gdb::Graph &graph,
                const snb::TypeCache &cache,
                std::priority_queue<Result,std::vector<Result>, mycomparison> &results, int limit) {
            ptree::ptree pt;
            int counter = 0;
            std::list<Result> temp;
            while(!results.empty()) {
                temp.push_front(results.top());
                results.pop();
            }
            for (std::list<Result>::const_iterator it = temp.begin();
                    it != temp.end() && counter < limit; ++it, ++counter) {
                pt.push_back(std::make_pair("", Project(graph, cache, *it)));
            }
            return pt;
        }

        datatypes::Buffer Execute(gdb::Session *sess, long long person_id,
                long long max_date, unsigned int limit) {
#ifdef VERBOSE
            printf("QUERY2: %lli %llu %u\n", person_id, max_date, limit);
            timeval start;
            gettimeofday(&start,NULL);
#endif

            BEGIN_EXCEPTION
            gdb::Graph *graph = sess->GetGraph();
            snb::TypeCache *cache = snb::TypeCache::instance(graph);
            std::priority_queue<Result,std::vector<Result>, mycomparison> intermediate_result;
            gdb::Value val;
            gdb::Value val2;
            BEGIN_TRANSACTION

            timeval startCore;
            gettimeofday(&startCore,NULL);

            gdb::oid_t person_oid = graph->FindObject(cache->person_id_t, gdb::Value().SetLong(person_id));
            gdb::Objects* friends = graph->Neighbors( person_oid, cache->knows_t, gdb::Outgoing );
            gdb::ObjectsIterator* iter_knows = friends->Iterator();
            while(iter_knows->HasNext()) {
                gdb::oid_t friend_oid = iter_knows->Next();
                gdb::Objects* friend_posts = graph->Neighbors( friend_oid, cache->post_has_creator_t, gdb::Ingoing );
                if( intermediate_result.size() >= limit ) {
                    gdb::Objects* aux = friend_posts;
                    friend_posts = graph->Select(cache->post_creation_date_t, gdb::Between, val.SetTimestamp(intermediate_result.top().creation_date), val2.SetTimestamp(max_date), friend_posts);
                    delete aux;
                }   
                gdb::ObjectsIterator* iter_posts = friend_posts->Iterator();
                while(iter_posts->HasNext()) {
                    gdb::oid_t post_oid = iter_posts->Next();
                    graph->GetAttribute( post_oid, cache->post_creation_date_t, val );
                    long long date = val.GetTimestamp();
                    const Result& top = intermediate_result.top();
                    if( date <= max_date && ( intermediate_result.size() < limit || date >= top.creation_date )  ) {
                        graph->GetAttribute( post_oid, cache->post_id_t, val );
                        long long post_id = val.GetLong();
                        Result res = {date, post_id, friend_oid, post_oid, kPost};
                        intermediate_result.push(res);
                        if(intermediate_result.size() > limit) intermediate_result.pop();
                    }
                }
                delete iter_posts;
                delete friend_posts;

                gdb::Objects* friend_comments = graph->Neighbors( friend_oid, cache->comment_has_creator_t, gdb::Ingoing );
                if( intermediate_result.size() >= limit ) {
                    gdb::Objects* aux = friend_comments;
                    friend_comments = graph->Select(cache->comment_creation_date_t, gdb::Between, val.SetTimestamp(intermediate_result.top().creation_date), val2.SetTimestamp(max_date) , friend_comments);
                    delete aux;
                } 
                
                gdb::ObjectsIterator* iter_comments = friend_comments->Iterator();
                while(iter_comments->HasNext()) {
                    gdb::oid_t comment_oid = iter_comments->Next();
                    graph->GetAttribute( comment_oid, cache->comment_creation_date_t, val );
                    long long date = val.GetTimestamp();
                    const Result& top = intermediate_result.top();
                    if( date <= max_date && ( intermediate_result.size() < limit || date >= top.creation_date)) {
                        graph->GetAttribute( comment_oid, cache->comment_id_t, val );
                        long long comment_id = val.GetLong();
                        Result res = {date, comment_id , friend_oid, comment_oid, kComment};
                        intermediate_result.push(res);
                        if(intermediate_result.size() > limit) intermediate_result.pop();
                    }
                }
                delete iter_comments;
                delete friend_comments;
            }
            delete iter_knows;
            delete friends;

            timeval endCore;
            gettimeofday(&endCore,NULL);

            ptree::ptree pt = Project(*graph, *cache, intermediate_result, limit);
            COMMIT_TRANSACTION
            delete graph;
            char* ret = snb::utils::to_json(pt);
#ifdef VERBOSE
            printf("EXIT QUERY2: %lli %llu %u\n", person_id, max_date, limit);
/*            timeval end;
            gettimeofday(&end,NULL);
            unsigned long executionTime = (end.tv_sec - start.tv_sec)*1000 + (end.tv_usec - start.tv_usec)/1000;
            unsigned long executionTimeCore = (endCore.tv_sec - startCore.tv_sec)*1000 + (endCore.tv_usec - startCore.tv_usec)/1000;
            printf("QUERY2:EXECUTION_TIME: %lu %lu\n",executionTime, executionTimeCore);
            */
#endif

            return datatypes::Buffer(ret, strlen(ret));
            END_EXCEPTION
        }
    }
}

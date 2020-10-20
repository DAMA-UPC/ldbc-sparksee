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
#include <queue>

namespace gdb = sparksee::gdb;
namespace snb = sparksee::snb;
namespace ptree = boost::property_tree;

namespace interactive {
    namespace query9 {

        struct Result {
            long long creation_date;
            long long message_id;
            gdb::oid_t message_oid;
            gdb::type_t type;
        };

        bool compare_result(const Result &res_a, const Result &res_b) {
            if( res_a.creation_date != res_b.creation_date ) {
                return res_a.creation_date > res_b.creation_date;
            }
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
            gdb::Value val;
            ptree::ptree pt;
            gdb::Graph& nc_graph = const_cast<gdb::Graph&>(graph);
            gdb::Objects* creator = nc_graph.Neighbors(result.message_oid, cache.post_has_creator_t, gdb::Outgoing );
            if( creator->Count() == 0 ) {
              delete creator;
              creator = nc_graph.Neighbors(result.message_oid, cache.comment_has_creator_t, gdb::Outgoing );
            }
            gdb::oid_t person_oid = creator->Any();
            delete creator;
            nc_graph.GetAttribute(person_oid, cache.person_id_t, val);
            pt.put<long long>("Person.id", val.GetLong());
            nc_graph.GetAttribute(person_oid, cache.person_first_name_t, val);
            pt.put<std::string>("Person.firstName", sparksee::utils::to_string(val.GetString()));
            nc_graph.GetAttribute(person_oid, cache.person_last_name_t, val);
            pt.put<std::string>("Person.lastName", sparksee::utils::to_string(val.GetString()));
            pt.put<long long>("Message.id", result.message_id );
            gdb::attr_t content_attr = result.type == cache.post_t ? cache.post_content_t : cache.comment_content_t;;
            nc_graph.GetAttribute(result.message_oid, content_attr, val);
            if( val.IsNull() || (!val.IsNull() && val.GetString().length() == 0)) {
                nc_graph.GetAttribute(result.message_oid, cache.post_image_file_t, val);
            }
            pt.put<std::string>("Message.content", sparksee::utils::to_string(val.GetString()));
            pt.put<long long>("Message.creationDate", result.creation_date);
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
            printf("QUERY9: %lli %lli %u\n", person_id, max_date, limit);

            timeval start;
            gettimeofday(&start,NULL);
#endif

            BEGIN_EXCEPTION


            gdb::Graph *graph = sess->GetGraph();
            snb::TypeCache *cache = snb::TypeCache::instance(graph);
            std::priority_queue<Result,std::vector<Result>, mycomparison> intermediate_result;
            gdb::Value val, val_max_date;
            val_max_date.SetTimestamp(max_date-1);
            val.SetLong(person_id);
            BEGIN_TRANSACTION

            timeval startCore;
            gettimeofday(&startCore,NULL);

            gdb::oid_t person_oid = graph->FindObject(cache->person_id_t, val);
            gdb::Objects* friends = sparksee::utils::k_hop(*sess, *graph,  person_oid, cache->knows_t, gdb::Outgoing, 2, false);
            unsigned long post_count = 0;
            gdb::ObjectsIterator* iter_friends = friends->Iterator();

            while(iter_friends->HasNext()) {
                gdb::oid_t friend_oid = iter_friends->Next();
                gdb::Objects* posts = graph->Neighbors( friend_oid, cache->post_has_creator_t, gdb::Ingoing);
                post_count += posts->Count();
                gdb::Objects* filtered_posts = NULL; 
                if( intermediate_result.size() < limit ) {
                    filtered_posts = graph->Select(cache->post_creation_date_t, gdb::LessEqual,val_max_date, posts); 
                } else {
                    const Result& top = intermediate_result.top();
                    val.SetTimestamp(top.creation_date);
                    filtered_posts = graph->Select(cache->post_creation_date_t, gdb::Between,val, val_max_date, posts); 
                }
                gdb::ObjectsIterator* iter_posts = filtered_posts->Iterator();
                while(iter_posts->HasNext()){
                    gdb::oid_t post_oid = iter_posts->Next();
                    graph->GetAttribute(post_oid, cache->post_creation_date_t, val);
                    long long creation_date = val.GetTimestamp();
                    const Result& top = intermediate_result.top();
                    if( intermediate_result.size() < limit || creation_date >= top.creation_date ) {
                        graph->GetAttribute(post_oid, cache->post_id_t, val);
                        long long post_id = val.GetLong();
                        Result res = {creation_date, post_id, post_oid, cache->post_t}; 
                        intermediate_result.push(res);
                        if(intermediate_result.size() > limit) intermediate_result.pop();
                    }
                }
                delete iter_posts;
                delete filtered_posts;
                delete posts;
            }
            delete iter_friends;

            unsigned long comment_count = 0;
            iter_friends = friends->Iterator();
            while(iter_friends->HasNext()) {
                gdb::oid_t friend_oid = iter_friends->Next();
                gdb::Objects* comments = graph->Neighbors( friend_oid, cache->comment_has_creator_t, gdb::Ingoing);
                comment_count += comments->Count();
                gdb::Objects* filtered_comments = NULL; 
                if( intermediate_result.size() < limit ) {
                    filtered_comments = graph->Select(cache->comment_creation_date_t, gdb::LessEqual,val_max_date, comments); 
                } else {
                    const Result& top = intermediate_result.top();
                    val.SetTimestamp(top.creation_date);
                    filtered_comments = graph->Select(cache->comment_creation_date_t, gdb::Between,val, val_max_date, comments); 
                }
                gdb::ObjectsIterator* iter_comments = filtered_comments->Iterator();
                while(iter_comments->HasNext()){
                    gdb::oid_t comment_oid = iter_comments->Next();
                    graph->GetAttribute(comment_oid, cache->comment_creation_date_t, val);
                    long long creation_date = val.GetTimestamp();
                    const Result& top = intermediate_result.top();
                    if( intermediate_result.size() < limit || creation_date >= top.creation_date ) {
                        graph->GetAttribute(comment_oid, cache->comment_id_t, val);
                        long long comment_id = val.GetLong();
                        Result res = {creation_date, comment_id, comment_oid, cache->comment_t}; 
                        intermediate_result.push(res);
                        if(intermediate_result.size() > limit) intermediate_result.pop();
                    }
                }
                delete iter_comments;
                delete filtered_comments;
                delete comments;
            }
            delete iter_friends;
            delete friends;

            timeval endCore;
            gettimeofday(&endCore,NULL);

            ptree::ptree pt= Project(*graph, *cache, intermediate_result, limit);
            COMMIT_TRANSACTION
            delete graph;
#ifdef VERBOSE
            printf("EXIT QUERY9: %lli %lli %u\n", person_id, max_date, limit);
            /*timeval end;
            gettimeofday(&end,NULL);
            unsigned long executionTime = (end.tv_sec - start.tv_sec)*1000 + (end.tv_usec - start.tv_usec)/1000;
            unsigned long executionTimeCore = (endCore.tv_sec - startCore.tv_sec)*1000 + (endCore.tv_usec - startCore.tv_usec)/1000;
            printf("QUERY9:EXECUTION_TIME: %lu %lu\n",executionTime, executionTimeCore);
            */
#endif
            char* res = snb::utils::to_json(pt);
            return datatypes::Buffer(res, strlen(res));
            END_EXCEPTION
        }
    }
}

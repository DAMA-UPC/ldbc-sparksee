
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
#include <climits>
#include <queue>

namespace gdb = sparksee::gdb;
namespace snb = sparksee::snb;
namespace ptree = boost::property_tree;

namespace interactive {
    namespace short2{

        struct Result{
            long long message_id;
            long long creation_date;
            gdb::oid_t message_oid;
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
            ptree::ptree pt;
            pt.put<long long>("Message.id", result.message_id);
            gdb::Graph& nc_graph = const_cast<gdb::Graph&>(graph);
            gdb::Value val;
            gdb::oid_t original_oid = result.message_oid;
            gdb::type_t type = nc_graph.GetObjectType(result.message_oid);
            if(type == cache.post_t) {
              nc_graph.GetAttribute(result.message_oid,cache.post_content_t, val);
              if( val.IsNull() || 
                  (!val.IsNull() && val.GetString().length() == 0)) {
                nc_graph.GetAttribute(result.message_oid, cache.post_image_file_t, val);
              }
              pt.put<std::string>("Message.content", sparksee::utils::to_string(val.GetString()));
              nc_graph.GetAttribute(result.message_oid, cache.post_creation_date_t, val);
              pt.put<long long>("Message.creationDate", val.GetTimestamp());
              pt.put<long long>("Post.id", result.message_id);
            } else {
              nc_graph.GetAttribute(result.message_oid,cache.comment_content_t, val);
              pt.put<std::string>("Message.content", sparksee::utils::to_string(val.GetString()));
              nc_graph.GetAttribute(result.message_oid, cache.comment_creation_date_t, val);
              pt.put<long long>("Message.creationDate", val.GetTimestamp());
              bool found = false;
              while(!found) {
                gdb::Objects* reply = nc_graph.Neighbors(original_oid, cache.reply_of_t, gdb::Outgoing);
                original_oid = reply->Any();
                delete reply;
                gdb::type_t type = nc_graph.GetObjectType(original_oid);
                if( type == cache.post_t ) found = true;
              }
              nc_graph.GetAttribute(original_oid, cache.post_id_t, val);
              pt.put<long long>("Post.id", val.GetLong());
            }
            gdb::Objects* original_authors = nc_graph.Neighbors(original_oid, cache.post_has_creator_t, gdb::Outgoing);
            gdb::oid_t original_author_oid = original_authors->Any();
            delete original_authors;
            nc_graph.GetAttribute(original_author_oid, cache.person_id_t, val);
            pt.put<long long>("Person.id", val.GetLong());
            nc_graph.GetAttribute(original_author_oid, cache.person_first_name_t, val);
            pt.put<std::string>("Person.firstName", sparksee::utils::to_string(val.GetString()));
            nc_graph.GetAttribute(original_author_oid, cache.person_last_name_t, val);
            pt.put<std::string>("Person.lastName", sparksee::utils::to_string(val.GetString()));
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

        datatypes::Buffer Execute(gdb::Session *sess, long long person_id) {
#ifdef VERBOSE
            printf("SHORT QUERY2:\n");
#endif

            BEGIN_EXCEPTION

            gdb::Graph *graph = sess->GetGraph();
            snb::TypeCache *cache = snb::TypeCache::instance(graph);
            std::priority_queue<Result,std::vector<Result>, mycomparison> intermediate_result;
            gdb::Value val;
            val.SetLong(person_id);
            unsigned int limit = 10;
            BEGIN_TRANSACTION
            gdb::oid_t person_oid = graph->FindObject(cache->person_id_t, val);
            gdb::Objects* posts = graph->Neighbors(person_oid, cache->post_has_creator_t, gdb::Ingoing);
            gdb::Objects* comments = graph->Neighbors(person_oid, cache->comment_has_creator_t, gdb::Ingoing);

            gdb::ObjectsIterator* iter_posts = posts->Iterator();
            while(iter_posts->HasNext()) {
              gdb::oid_t post_oid = iter_posts->Next();
              Result res;
              graph->GetAttribute(post_oid, cache->post_creation_date_t, val);
              res.creation_date = val.GetTimestamp();
              const Result& top = intermediate_result.top();
              if( intermediate_result.size() < limit || top.creation_date < res.creation_date) {
                graph->GetAttribute(post_oid, cache->post_id_t, val);
                res.message_id = val.GetLong();
                res.message_oid = post_oid;
                intermediate_result.push(res);
                if( intermediate_result.size() > limit ) intermediate_result.pop();
              }
            }


            gdb::ObjectsIterator* iter_comments = comments->Iterator();
            while(iter_comments->HasNext()) {
              gdb::oid_t comment_oid = iter_comments->Next();
              Result res;
              graph->GetAttribute(comment_oid, cache->comment_creation_date_t, val);
              res.creation_date = val.GetTimestamp();
              const Result& top = intermediate_result.top();
              if( intermediate_result.size() < limit || top.creation_date < res.creation_date) {
                graph->GetAttribute(comment_oid, cache->comment_id_t, val);
                res.message_id = val.GetLong();
                res.message_oid = comment_oid;
                intermediate_result.push(res);
                if( intermediate_result.size() > limit ) intermediate_result.pop();
              }
            }
            delete iter_comments;
            delete iter_posts;
            delete posts;
            delete comments;
            ptree::ptree pt = Project(*graph, *cache, intermediate_result, limit);
            COMMIT_TRANSACTION
            delete graph;
#ifdef VERBOSE
            printf("EXIT SHORT QUERY2:\n");
#endif
            char* res = snb::utils::to_json(pt);
            return datatypes::Buffer(res, strlen(res));
            END_EXCEPTION

        }
    }
}

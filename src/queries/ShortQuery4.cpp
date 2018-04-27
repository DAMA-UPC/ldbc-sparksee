
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
    namespace short4{

        struct Result {
            std::string content;
            long long creation_date;
        };

        static ptree::ptree Project(const gdb::Graph &graph,
                const snb::TypeCache &cache, const Result &result) {
            ptree::ptree pt;
            pt.put<std::string>("Message.content", result.content);
            pt.put<long long>("Message.creationDate", result.creation_date);
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
            printf("SHORT QUERY4:\n");
#endif
            BEGIN_EXCEPTION
            gdb::Graph *graph = sess->GetGraph();
            snb::TypeCache *cache = snb::TypeCache::instance(graph);
            std::vector<Result> intermediate_result;
            gdb::Value val;
            val.SetLong(message_id);
            BEGIN_TRANSACTION
            gdb::oid_t message_oid = graph->FindObject(cache->comment_id_t, val);
            long long creation_date = 0;
            gdb::attr_t content_attr = cache->comment_content_t;
            if( message_oid != gdb::Objects::InvalidOID ) {
              graph->GetAttribute(message_oid, cache->comment_creation_date_t, val);
              creation_date = val.GetTimestamp();
            } else {
              message_oid = graph->FindObject(cache->post_id_t, val);
              content_attr = cache->post_content_t;
              graph->GetAttribute(message_oid, cache->post_creation_date_t, val);
              creation_date = val.GetTimestamp();
            }
            Result res;
            graph->GetAttribute(message_oid, content_attr, val);
            if(val.IsNull()){
                graph->GetAttribute(message_oid, cache->post_image_file_t, val); 
            }
            res.content = sparksee::utils::to_string(val.GetString());
            res.creation_date = creation_date;
            intermediate_result.push_back(res);
            ptree::ptree pt = Project(*graph, *cache, intermediate_result, 1);
            COMMIT_TRANSACTION
            delete graph;
#ifdef VERBOSE
            printf("EXIT SHORT QUERY4:\n");
#endif
            char* ret = snb::utils::to_json(pt);
            return datatypes::Buffer(ret, strlen(ret));
            END_EXCEPTION
        }
    }
}

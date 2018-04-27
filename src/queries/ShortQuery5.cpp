
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
    namespace short5{

        struct Result {
            long long person_id;
            std::string first_name;
            std::string last_name;
        };

        static ptree::ptree Project(const gdb::Graph &graph,
                const snb::TypeCache &cache, const Result &result) {
            ptree::ptree pt;
            pt.put<long long>("Person.id", result.person_id);
            pt.put<std::string>("Person.firstName", result.first_name);
            pt.put<std::string>("Person.lastName", result.last_name);
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

/*        static datatypes::Buffer Project(const gdb::Graph &graph,
                const snb::TypeCache &cache,
                const std::vector<Result> &results, int limit) {
            int buffer_size = 1024;
            char* res = new char[buffer_size];
            int position = 0;
            int num_elements = results.size() >=20 ? limit : results.size();
            sparksee::utils::write<int>(&res, buffer_size, position, num_elements );
            for( int i = 0; i < num_elements; ++i ) {
              snb::utils::write<long long>(&res, buffer_size, position, results[i].person_id);
              snb::utils::write_str(&res, buffer_size, position, results[i].first_name);
              snb::utils::write_str(&res, buffer_size, position, results[i].last_name);
            }
            return datatypes::Buffer(res,position, datatypes::E_RAW );
        }
        */

        datatypes::Buffer Execute(gdb::Session *sess, long long message_id) {
#ifdef VERBOSE
            printf("SHORT QUERY5:\n");
#endif
            BEGIN_EXCEPTION

            gdb::Graph *graph = sess->GetGraph();
            snb::TypeCache *cache = snb::TypeCache::instance(graph);
            std::vector<Result> intermediate_result;
            gdb::Value val;
            val.SetLong(message_id);
            BEGIN_TRANSACTION
            gdb::oid_t message_oid = graph->FindObject(cache->post_id_t, val);
            if( message_oid == gdb::Objects::InvalidOID ) {
                message_oid = graph->FindObject(cache->comment_id_t, val);
            }
            gdb::Objects* creators = graph->Neighbors(message_oid, cache->post_has_creator_t, gdb::Outgoing);
            if( creators->Count() == 0 ) {
              delete creators;
              creators = graph->Neighbors(message_oid, cache->comment_has_creator_t, gdb::Outgoing);
            }
            gdb::oid_t person_oid = creators->Any();
            Result res;
            graph->GetAttribute(person_oid, cache->person_id_t, val);
            res.person_id = val.GetLong();
            graph->GetAttribute(person_oid, cache->person_first_name_t, val);
            res.first_name = sparksee::utils::to_string(val.GetString());
            graph->GetAttribute(person_oid, cache->person_last_name_t, val);
            res.last_name = sparksee::utils::to_string(val.GetString());
            intermediate_result.push_back(res);
            delete creators;
            ptree::ptree pt = Project(*graph, *cache, intermediate_result, 1);
            //datatypes::Buffer buf =  Project(*graph, *cache, intermediate_result, 1);
            COMMIT_TRANSACTION
            delete graph;
#ifdef VERBOSE
            printf("EXIT SHORT QUERY5:\n");
#endif
            char* ret = snb::utils::to_json(pt);
            return datatypes::Buffer(ret, strlen(ret));
            //return buf;
            END_EXCEPTION
        }
    }
}

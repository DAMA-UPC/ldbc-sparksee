
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

namespace interactive {
    namespace short3{

        namespace gdb = sparksee::gdb;
        namespace snb = sparksee::snb;
        namespace ptree = boost::property_tree;

        struct Result {
            long long person_id;
            long long creation_date;
            gdb::oid_t person_oid;
        };

        bool compare_result(const Result &res_a, const Result &res_b) {
            if( res_a.creation_date != res_b.creation_date ) {
                return res_a.creation_date > res_b.creation_date;
            }
            return res_a.person_id < res_b.person_id;
        }

        static ptree::ptree Project(const gdb::Graph &graph,
                const snb::TypeCache &cache, const Result &result) {
            ptree::ptree pt;
            gdb::Graph& nc_graph = const_cast<gdb::Graph&>(graph);
            gdb::Value val;
            pt.put<long long>("Person.id", result.person_id);
            nc_graph.GetAttribute(result.person_oid, cache.person_first_name_t, val);
            pt.put<std::string>("Person.firstName", sparksee::utils::to_string(val.GetString()));
            nc_graph.GetAttribute(result.person_oid, cache.person_last_name_t, val);
            pt.put<std::string>("Person.lastName", sparksee::utils::to_string(val.GetString()));
            pt.put<long long>("Knows.creationDate", result.creation_date);
            return pt;
        }

        static ptree::ptree Project(const gdb::Graph &graph,
                const snb::TypeCache &cache,
                const std::vector<Result> &results) {
            ptree::ptree pt;
            for (std::vector<Result>::const_iterator it = results.begin();
                    it != results.end(); ++it) {
                pt.push_back(std::make_pair("", Project(graph, cache, *it)));
            }
            return pt;
        }

        datatypes::Buffer Execute(gdb::Session *sess, long long person_id) {
#ifdef VERBOSE
            printf("SHORT QUERY3: %llu \n", person_id);
#endif

            BEGIN_EXCEPTION

            gdb::Graph *graph = sess->GetGraph();
            snb::TypeCache *cache = snb::TypeCache::instance(graph);
            std::vector<Result> intermediate_result;
            gdb::Value val;
            val.SetLong(person_id);
            BEGIN_TRANSACTION
            gdb::oid_t person_oid = graph->FindObject(cache->person_id_t, val);
            gdb::Objects* friendships = graph->Explode(person_oid, cache->knows_t, gdb::Outgoing); 
            gdb::ObjectsIterator* iter_friendships = friendships->Iterator();
            while(iter_friendships->HasNext()) {
                gdb::oid_t friendship_oid = iter_friendships->Next();
                gdb::oid_t friend_oid = graph->GetEdgePeer(friendship_oid, person_oid);
                Result res;
                graph->GetAttribute(friend_oid, cache->person_id_t, val);
                res.person_id = val.GetLong();
                graph->GetAttribute(friendship_oid, cache->knows_creation_date_t, val);
                res.creation_date = val.GetTimestamp();
                res.person_oid = friend_oid;
                intermediate_result.push_back(res);
            }
            delete iter_friendships;
            delete friendships;
            std::sort(intermediate_result.begin(), intermediate_result.end(),
                    compare_result);
            ptree::ptree pt = Project(*graph, *cache, intermediate_result);
            COMMIT_TRANSACTION
            delete graph;
#ifdef VERBOSE
            printf("EXIT SHORT QUERY3: %llu \n", person_id);
#endif
            char* res = snb::utils::to_json(pt);
            return datatypes::Buffer(res,strlen(res));
            END_EXCEPTION
        }
    }
}

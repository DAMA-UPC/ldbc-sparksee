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
    namespace query11 {

        struct Result {
            int year;
            long long person_id;
            gdb::oid_t person_oid;
            gdb::oid_t company_oid;
            std::string company_name;
        };

        bool compare_result(const Result &res_a, const Result &res_b) {
            if( res_a.year != res_b.year ) {
                return res_a.year < res_b.year;
            }

            if( res_a.person_id != res_b.person_id ) {
                return res_a.person_id < res_b.person_id;
            }
            return res_a.company_name > res_b.company_name; 
        }

        static ptree::ptree Project(const gdb::Graph &graph,
                const snb::TypeCache &cache, const Result &result) {
            gdb::Value val;
            ptree::ptree pt;
            gdb::Graph& nc_graph = const_cast<gdb::Graph&>(graph);
            nc_graph.GetAttribute(result.person_oid, cache.person_id_t, val);
            pt.put<long long>("Person.id", val.GetLong());
            nc_graph.GetAttribute(result.person_oid, cache.person_first_name_t, val);
            pt.put<std::string>("Person.firstName", sparksee::utils::to_string(val.GetString()));
            nc_graph.GetAttribute(result.person_oid, cache.person_last_name_t, val);
            pt.put<std::string>("Person.lastName", sparksee::utils::to_string(val.GetString()));
            pt.put<std::string>("worksAt.name", result.company_name );
            pt.put<int>("worksAt.worksFrom", result.year);
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
                const char *country, int year, unsigned int limit) {
#ifdef VERBOSE
            printf("QUERY11: %lli %s %i %u\n", person_id, country, year, limit);
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
            gdb::Objects* friends = sparksee::utils::k_hop(*sess, *graph, person_oid, cache->knows_t, gdb::Outgoing, 2, false);
            gdb::ObjectsIterator* iter_friends = friends->Iterator();
            while(iter_friends->HasNext()) {
                gdb::oid_t friend_oid = iter_friends->Next();
                gdb::Objects* work_ats = graph->Explode(friend_oid, cache->work_at_t, gdb::Outgoing);
                gdb::ObjectsIterator* iter_work_ats = work_ats->Iterator();
                while(iter_work_ats->HasNext()){
                    gdb::oid_t work_at_oid = iter_work_ats->Next();
                    graph->GetAttribute( work_at_oid, cache->work_at_work_from_t, val);
                    int work_at_year = val.GetInteger();
                    if( work_at_year < year ) {
                        gdb::EdgeData* e_data = graph->GetEdgeData(work_at_oid);
                        gdb::oid_t company_oid = e_data->GetHead();
                        gdb::Objects* company_place = graph->Neighbors(company_oid, cache->is_located_in_t, gdb::Outgoing);
                        graph->GetAttribute(company_place->Any(), cache->place_name_t, val);
                        delete company_place;
                        if( sparksee::utils::to_string(val.GetString()) == country ) {
                            graph->GetAttribute( friend_oid, cache->person_id_t, val);
                            long long friend_id = val.GetLong();
                            graph->GetAttribute( company_oid, cache->organization_name_t, val);
                            Result res = {work_at_year, friend_id, friend_oid, company_oid, sparksee::utils::to_string(val.GetString())};
                            intermediate_result.push_back(res);
                        }
                        delete e_data;
                    } 
                }
                delete iter_work_ats;
                delete work_ats;
            }
            delete iter_friends;
            delete friends;

            timeval endCore;
            gettimeofday(&endCore,NULL);

            std::sort(intermediate_result.begin(), intermediate_result.end(),
                    compare_result);
            ptree::ptree pt = Project(*graph, *cache, intermediate_result, limit);
            COMMIT_TRANSACTION
            delete graph;
#ifdef VERBOSE
            printf("EXIT QUERY11: %lli %s %i %u\n", person_id, country, year, limit);
            /*
            timeval end;
            gettimeofday(&end,NULL);
            unsigned long executionTime = (end.tv_sec - start.tv_sec)*1000 + (end.tv_usec - start.tv_usec)/1000;
            unsigned long executionTimeCore = (endCore.tv_sec - startCore.tv_sec)*1000 + (endCore.tv_usec - startCore.tv_usec)/1000;
            printf("QUERY11:EXECUTION_TIME: %lu %lu\n",executionTime, executionTimeCore);
            */
#endif
            char* res = snb::utils::to_json(pt);
            return datatypes::Buffer(res, strlen(res));
            END_EXCEPTION
        }
    }
}

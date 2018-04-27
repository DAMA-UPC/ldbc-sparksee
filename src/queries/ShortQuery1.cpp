
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
    namespace short1{

        struct Result {
            std::string first_name;
            std::string last_name;
            std::string location_ip;
            std::string browser_used;
            long long birthday;
            long long city_id;
            std::string gender;
            long creation_date;
        };

        static ptree::ptree Project(const gdb::Graph &graph,
                const snb::TypeCache &cache, const Result &result) {
            ptree::ptree pt;
            pt.put<std::string>("Person.firstName", result.first_name);
            pt.put<std::string>("Person.lastName", result.last_name);
            pt.put<long long>("Person.birthday", result.birthday);
            pt.put<std::string>("Person.locationIP", result.location_ip);
            pt.put<std::string>("Person.browserUsed", result.browser_used);
            pt.put<long long>("Person.cityId", result.city_id);
            pt.put<std::string>("Person.gender", result.gender);
            pt.put<long long>("Person.creationDate", result.creation_date);
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

        datatypes::Buffer Execute(gdb::Session *sess, long long person_id) {
#ifdef VERBOSE
            printf("SHORT QUERY1:\n");
#endif

            BEGIN_EXCEPTION

            gdb::Graph *graph = sess->GetGraph();
            snb::TypeCache *cache = snb::TypeCache::instance(graph);
            std::vector<Result> intermediate_result;
            gdb::Value val;
            val.SetLong(person_id);
            BEGIN_TRANSACTION
            gdb::oid_t person_oid = graph->FindObject(cache->person_id_t, val);

            Result res;
            graph->GetAttribute(person_oid, cache->person_first_name_t, val);
            res.first_name = sparksee::utils::to_string(val.GetString());
            graph->GetAttribute(person_oid, cache->person_last_name_t, val);
            res.last_name = sparksee::utils::to_string(val.GetString());
            graph->GetAttribute(person_oid, cache->person_birthday_t, val);
            res.birthday = sparksee::utils::parse_date(sparksee::utils::to_string(val.GetString()));;
            graph->GetAttribute(person_oid, cache->person_locationIP_t, val);
            res.location_ip = sparksee::utils::to_string(val.GetString());
            graph->GetAttribute(person_oid, cache->person_browser_used_t, val);
            res.browser_used = sparksee::utils::to_string(val.GetString());
            gdb::Objects* cities = graph->Neighbors(person_oid, cache->is_located_in_t, gdb::Outgoing);
            graph->GetAttribute(cities->Any(), cache->place_id_t, val);
            res.city_id = val.GetLong();
            graph->GetAttribute(person_oid, cache->person_gender_t, val);
            res.gender = sparksee::utils::to_string(val.GetString());
            graph->GetAttribute(person_oid, cache->person_creation_date_t, val);
            res.creation_date = val.GetTimestamp();
            intermediate_result.push_back(res);
            delete cities;
            ptree::ptree pt = Project(*graph, *cache, intermediate_result, 1);
            COMMIT_TRANSACTION
            delete graph;
#ifdef VERBOSE
            printf("EXIT SHORT QUERY1:\n");
#endif
            char* ret = snb::utils::to_json(pt);
            return datatypes::Buffer(ret, strlen(ret));
            END_EXCEPTION
        }
    }
}

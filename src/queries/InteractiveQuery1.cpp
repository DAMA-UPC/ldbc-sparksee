#include "Database.h"
#include "TypeCache.h"
#include "Utils.h"
#include <utils/Utils.h>
#include "snbInteractive.h"
#include <gdb/Graph.h>
#include <gdb/Objects.h>
#include <gdb/ObjectsIterator.h>
#include <gdb/Session.h>
#include <gdb/Sparksee.h>
#include <gdb/Value.h>
#include <stdio.h>
#include <string>
#include <cstring>
#include <vector>
#include <algorithm>
#include <boostPatch/property_tree/ptree.hpp>
#include <boostPatch/property_tree/json_parser.hpp>
#include <time.h>

namespace gdb = sparksee::gdb;
namespace snb = sparksee::snb;
namespace ptree = boost::property_tree;

namespace interactive {
    namespace query1 {

        struct Result {
            long long id;
            long long oid;
            int distance;
            std::string last_name;
        };

        bool compare_result(const Result &res_a, const Result &res_b) {
            if (res_a.distance != res_b.distance)
                return res_a.distance < res_b.distance;
            if (res_a.last_name != res_b.last_name)
                return res_a.last_name < res_b.last_name;
            return res_a.id < res_b.id;
        }

        static ptree::ptree Project(const gdb::Graph &graph,
                const snb::TypeCache &cache, const Result &result) {
            gdb::Graph &nc_graph = const_cast<gdb::Graph &>(graph);
            gdb::Value val;
            ptree::ptree pt;

            pt.put<long long>("Person.id", result.id);
            pt.put<std::string>("Person.lastName", result.last_name);
            pt.put<int>("distance", result.distance);

            nc_graph.GetAttribute(result.oid, cache.person_birthday_t, val);
            pt.put<long long>("Person.birthday", sparksee::utils::parse_date(sparksee::utils::to_string(val.GetString())));
            nc_graph.GetAttribute(result.oid, cache.person_creation_date_t, val);
            pt.put<long long>("Person.creationDate", val.GetTimestamp());

            nc_graph.GetAttribute(result.oid, cache.person_gender_t, val);
            pt.put<std::string>("Person.gender", sparksee::utils::to_string(val.GetString()));
            nc_graph.GetAttribute(result.oid, cache.person_browser_used_t, val);
            pt.put<std::string>("Person.browserUsed",
                    sparksee::utils::to_string(val.GetString()));
            nc_graph.GetAttribute(result.oid, cache.person_locationIP_t, val);
            pt.put<std::string>("Person.locationIP",
                    sparksee::utils::to_string(val.GetString()));
            gdb::Objects *emails =
                nc_graph.Neighbors(result.oid, cache.email_t, gdb::Outgoing);
            gdb::ObjectsIterator *it = emails->Iterator();
            ptree::ptree emails_pt;
            while (it->HasNext()) {
                gdb::oid_t email = it->Next();
                nc_graph.GetAttribute(email, cache.email_address_email_address_t, val);
                emails_pt.push_back(std::make_pair("", sparksee::utils::to_string(val.GetString())));
            }
            delete it;
            delete emails;
            pt.get_child("Person").push_back(std::make_pair("emails", emails_pt));

            gdb::Objects *languages =
                nc_graph.Neighbors(result.oid, cache.speaks_t, gdb::Outgoing);
            it = languages->Iterator();
            ptree::ptree languages_pt;
            while (it->HasNext()) {
                gdb::oid_t language = it->Next();
                nc_graph.GetAttribute(language, cache.language_language_t, val);
                languages_pt.push_back(
                        std::make_pair("", sparksee::utils::to_string(val.GetString())));
            }
            delete it;
            delete languages;
            pt.get_child("Person").push_back(std::make_pair("languages", languages_pt));

            gdb::Objects *places =
                nc_graph.Neighbors(result.oid, cache.is_located_in_t, gdb::Outgoing);
            nc_graph.GetAttribute(places->Any(), cache.place_name_t, val);
            pt.put<std::string>("Person.Place.name",sparksee::utils::to_string(val.GetString()));
            delete places;

            ptree::ptree study_ats_pt;
            gdb::Objects *study_ats =
                nc_graph.Explode(result.oid, cache.study_at_t, gdb::Outgoing);
            it = study_ats->Iterator();
            while (it->HasNext()) {
                gdb::oid_t study_at_oid = it->Next();
                ptree::ptree study_at_pt;
                gdb::oid_t university_oid = nc_graph.GetEdgePeer(study_at_oid, result.oid);
                nc_graph.GetAttribute(university_oid, cache.organization_name_t, val);
                study_at_pt.put<std::string>("University.name",
                        sparksee::utils::to_string(val.GetString()));
                nc_graph.GetAttribute(study_at_oid, cache.study_at_classyear_t, val);
                study_at_pt.put<int>("classYear", val.GetInteger());
                gdb::Objects *places = nc_graph.Neighbors(
                        university_oid, cache.is_located_in_t, gdb::Outgoing);
                nc_graph.GetAttribute(places->Any(), cache.place_name_t, val);
                study_at_pt.put<std::string>("City.name",
                        sparksee::utils::to_string(val.GetString()));
                study_ats_pt.push_back(std::make_pair("", study_at_pt));
                delete places;
            }
            delete it;
            delete study_ats;
            pt.get_child("Person").push_back(std::make_pair("studyAt", study_ats_pt));

            ptree::ptree work_ats_pt;
            gdb::Objects *work_ats =
                nc_graph.Explode(result.oid, cache.work_at_t, gdb::Outgoing);
            it = work_ats->Iterator();
            while (it->HasNext()) {
                gdb::oid_t work_at_oid = it->Next();
                ptree::ptree work_at_pt;
                gdb::oid_t university_oid = nc_graph.GetEdgePeer(work_at_oid, result.oid);
                nc_graph.GetAttribute(university_oid, cache.organization_name_t, val);
                work_at_pt.put<std::string>("Company.name",
                        sparksee::utils::to_string(val.GetString()));
                nc_graph.GetAttribute(work_at_oid, cache.work_at_work_from_t, val);
                work_at_pt.put<int>("workFrom", val.GetInteger());
                gdb::Objects *places = nc_graph.Neighbors(
                        university_oid, cache.is_located_in_t, gdb::Outgoing);
                nc_graph.GetAttribute(places->Any(), cache.place_name_t, val);
                work_at_pt.put<std::string>("Country.name",
                        sparksee::utils::to_string(val.GetString()));
                work_ats_pt.push_back(std::make_pair("", work_at_pt));
                delete places;
            }
            delete it;
            delete work_ats;
            pt.get_child("Person").push_back(std::make_pair("workAt", work_ats_pt));
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
                const char *first_name, unsigned int limit) {
#ifdef VERBOSE
            printf("QUERY1: %lli %s %u\n", person_id, first_name, limit);
            timeval start;
            gettimeofday(&start,NULL);
#endif


            BEGIN_EXCEPTION


            std::vector<Result> intermediate_result;
            gdb::Graph *graph = sess->GetGraph();
            snb::TypeCache *cache = snb::TypeCache::instance(graph);
            BEGIN_TRANSACTION;
            timeval startCore;
            gettimeofday(&startCore,NULL);
            long person_oid =
                graph->FindObject(cache->person_id_t, gdb::Value().SetLong(person_id));
            gdb::Value first_name_val;
            first_name_val.SetString(sparksee::utils::to_wstring(first_name));
            gdb::Objects *visited = sess->NewObjects();
            visited->Add(person_oid);
            for (int distance = 1; distance < 3 && intermediate_result.size() < limit;
                    ++distance) {
                gdb::Objects *k_hop_neighbors = sparksee::utils::k_hop(
                        *sess, *graph, person_oid, cache->knows_t, gdb::Outgoing, distance, false);
                k_hop_neighbors->Difference(visited);
                gdb::Objects *filter =
                    graph->Select(cache->person_first_name_t, gdb::Equal, first_name_val,
                            k_hop_neighbors);
                gdb::ObjectsIterator *it = filter->Iterator();
                while (it->HasNext()) {
                    Result res;
                    gdb::oid_t oid = it->Next();
                    res.oid = oid;
                    res.distance = distance;
                    gdb::Value value;
                    graph->GetAttribute(oid, cache->person_id_t, value);
                    res.id = value.GetLong();
                    graph->GetAttribute(oid, cache->person_last_name_t, value);
                    res.last_name = sparksee::utils::to_string(value.GetString());
                    intermediate_result.push_back(res);
                }
                delete it;
                delete filter;
                visited->Union(k_hop_neighbors);
                delete k_hop_neighbors;
            }
            if( intermediate_result.size() < limit ) {
                gdb::Objects* candidates = graph->Select(cache->person_first_name_t, gdb::Equal, first_name_val);
                candidates->Difference(visited);
                gdb::ObjectsIterator* iter_candidates = candidates->Iterator();
                while(iter_candidates->HasNext()) {
                    gdb::oid_t id = iter_candidates->Next();
                    gdb::Objects* neighbors = graph->Neighbors(id, cache->knows_t, gdb::Outgoing); 
                        neighbors->Intersection(visited);
                        if( neighbors->Count() != 0 ) {
                            Result res;
                            gdb::oid_t oid = id;
                            res.oid = oid;
                            res.distance = 3;
                            gdb::Value value;
                            graph->GetAttribute(oid, cache->person_id_t, value);
                            res.id = value.GetLong();
                            graph->GetAttribute(oid, cache->person_last_name_t, value);
                            res.last_name = sparksee::utils::to_string(value.GetString());
                            intermediate_result.push_back(res);
                        }
                    delete neighbors;
                }
                delete iter_candidates;
                delete candidates;
            }
            delete visited;
            timeval endCore;
            gettimeofday(&endCore,NULL);
            std::sort(intermediate_result.begin(), intermediate_result.end(),
                    compare_result);
            ptree::ptree pt = Project(*graph, *cache, intermediate_result, limit);
            COMMIT_TRANSACTION
            delete graph;


#ifdef VERBOSE
            printf("EXIT QUERY1: %lli %s %u\n", person_id, first_name, limit);
            /*
            timeval end;
            gettimeofday(&end,NULL);
            unsigned long executionTime = (end.tv_sec - start.tv_sec)*1000 + (end.tv_usec - start.tv_usec)/1000;
            unsigned long executionTimeCore = (endCore.tv_sec - startCore.tv_sec)*1000 + (endCore.tv_usec - startCore.tv_usec)/1000;
            printf("QUERY1:EXECUTION_TIME: %lu %lu\n",executionTime, executionTimeCore);
            */
#endif

  //          ptree::ptree pt;
            char* res = snb::utils::to_json(pt);
            return datatypes::Buffer(res, strlen(res));
            END_EXCEPTION
        }
    }
}

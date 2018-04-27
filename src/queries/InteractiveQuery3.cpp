
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
#include <boost/scoped_ptr.hpp>
#include <time.h>

namespace gdb = sparksee::gdb;
namespace snb = sparksee::snb;
namespace ptree = boost::property_tree;

namespace interactive {
    namespace query3 {

        struct Result {
            long long person_id;
            gdb::oid_t person_oid;
            long long num_messagesX;
            long long num_messagesY;
        };

        bool compare_result(const Result &res_a, const Result &res_b) {
            long total_a = res_a.num_messagesX + res_a.num_messagesY;
            long total_b = res_b.num_messagesX + res_b.num_messagesY;
            if (total_a != total_b )  
                return total_a > total_b;
            return res_a.person_id < res_b.person_id;
        }

        static ptree::ptree Project(const gdb::Graph &graph,
                const snb::TypeCache &cache, const Result &result) {
            gdb::Graph &nc_graph = const_cast<gdb::Graph &>(graph);
            gdb::Value val;
            ptree::ptree pt;
            pt.put<long long>("Person.id", result.person_id);
            nc_graph.GetAttribute( result.person_oid, cache.person_first_name_t, val);
            pt.put<std::string>("Person.firstName", sparksee::utils::to_string(val.GetString()));
            nc_graph.GetAttribute( result.person_oid, cache.person_last_name_t, val);
            pt.put<std::string>("Person.lastName", sparksee::utils::to_string(val.GetString()));
            pt.put<long long>("countx", result.num_messagesX );
            pt.put<long long>("county", result.num_messagesY );
            pt.put<long long>("count", result.num_messagesX + result.num_messagesY );
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

        typedef struct PersonCount_ {
          gdb::oid_t person_oid;
          long long count;
        } PersonCount;

        bool compare_person_count(const PersonCount &a, const PersonCount &b) {
            return sparksee::utils::descending<long long>(a.count, b.count);
        }

        datatypes::Buffer Execute(gdb::Session *sess, long long person_id,
                const char *country1, const char *country2, long long date,
                unsigned int duration, unsigned int limit) {
#ifdef VERBOSE
            printf("QUERY3: %lld %s %s %u\n", person_id, country1, country2, limit);
            timeval start;
            gettimeofday(&start,NULL);
#endif
            BEGIN_EXCEPTION

            sparksee::utils::GraphPtr graph(sess->GetGraph());
            snb::TypeCache *cache = snb::TypeCache::instance(graph.get());
            std::vector<Result> intermediate_result;
            gdb::Value start_date;
            start_date.SetTimestamp(date);
            gdb::Value end_date;
            end_date.SetTimestamp(date + ((long long int)duration)*24L*3600L*1000L);
            gdb::Value val;
            std::wstring wcountry1 = sparksee::utils::to_wstring(country1);
            std::wstring wcountry2 = sparksee::utils::to_wstring(country2);
            BEGIN_TRANSACTION
            gdb::oid_t person_oid = graph->FindObject(cache->person_id_t, val.SetLong(person_id));
            gdb::oid_t country1_oid = graph->FindObject(cache->place_name_t, val.SetString(wcountry1));
            gdb::oid_t country2_oid = graph->FindObject(cache->place_name_t, val.SetString(wcountry2));
            graph->GetAttribute(country1_oid, cache->place_id_t, val);
            long long country1_id = val.GetLong();
            graph->GetAttribute(country2_oid, cache->place_id_t, val);
            long long country2_id = val.GetLong();
            gdb::Value countryDateStart1, countryDateStart2, countryDateEnd1, countryDateEnd2;
            countryDateStart1.SetLong(snb::utils::country_date(country1_id,start_date.GetTimestamp()));
            countryDateEnd1.SetLong(snb::utils::country_date(country1_id,end_date.GetTimestamp()) + 1024);
            countryDateStart2.SetLong(snb::utils::country_date(country2_id,start_date.GetTimestamp()));
            countryDateEnd2.SetLong(snb::utils::country_date(country2_id,end_date.GetTimestamp()) + 1024);

            sparksee::utils::ObjectsPtr cities_country1(graph->Neighbors(country1_oid, cache->is_part_of_t, gdb::Ingoing));
            sparksee::utils::ObjectsPtr cities_country2(graph->Neighbors(country2_oid, cache->is_part_of_t, gdb::Ingoing));

            sparksee::utils::ObjectsPtr filtered_posts_1(graph->Select( cache->post_country_date_t, gdb::Between, countryDateStart1, countryDateEnd1));
            sparksee::utils::ObjectsPtr filtered_comments_1(graph->Select( cache->comment_country_date_t, gdb::Between, countryDateStart1, countryDateEnd1));

            timeval startCore;
            gettimeofday(&startCore,NULL);

            std::map<gdb::oid_t, unsigned long> person_message_count1;
            sparksee::utils::ObjectsIteratorPtr iter_posts_1(filtered_posts_1->Iterator()); 
            while(iter_posts_1->HasNext()) {
              gdb::oid_t message_oid = iter_posts_1->Next();
              gdb::oid_t creator_oid = sparksee::utils::ObjectsPtr(graph->Neighbors(message_oid, cache->post_has_creator_t, gdb::Outgoing))->Any();
              gdb::oid_t person_city = sparksee::utils::ObjectsPtr(graph->Neighbors(creator_oid, cache->is_located_in_t, gdb::Outgoing))->Any();
              if(!cities_country1->Exists(person_city) && !cities_country2->Exists(person_city)) {
                graph->GetAttribute(message_oid, cache->post_creation_date_t, val);
                if( val.GetTimestamp() >= start_date.GetTimestamp() && val.GetTimestamp() <= end_date.GetTimestamp() )
                  person_message_count1[creator_oid]++;
              }
            }

            sparksee::utils::ObjectsIteratorPtr iter_comments_1(filtered_comments_1->Iterator()); 
            while(iter_comments_1->HasNext()) {
              gdb::oid_t message_oid = iter_comments_1->Next();
              gdb::oid_t creator_oid = sparksee::utils::ObjectsPtr(graph->Neighbors(message_oid, cache->comment_has_creator_t, gdb::Outgoing))->Any();
              gdb::oid_t person_city = sparksee::utils::ObjectsPtr(graph->Neighbors(creator_oid, cache->is_located_in_t, gdb::Outgoing))->Any();
              if(!cities_country1->Exists(person_city) && !cities_country2->Exists(person_city)) {
                graph->GetAttribute(message_oid, cache->comment_creation_date_t, val);
                if( val.GetTimestamp() >= start_date.GetTimestamp() && val.GetTimestamp() <= end_date.GetTimestamp() )
                person_message_count1[creator_oid]++;
              }
            }

            sparksee::utils::ObjectsPtr filtered_posts_2(graph->Select( cache->post_country_date_t, gdb::Between, countryDateStart2, countryDateEnd2));
            sparksee::utils::ObjectsPtr filtered_comments_2(graph->Select( cache->comment_country_date_t, gdb::Between, countryDateStart2, countryDateEnd2));
            std::map<gdb::oid_t, unsigned long> person_message_count2;
            sparksee::utils::ObjectsIteratorPtr iter_posts_2(filtered_posts_2->Iterator()); 
            while(iter_posts_2->HasNext()) {
              gdb::oid_t message_oid = iter_posts_2->Next();
              gdb::oid_t creator_oid = sparksee::utils::ObjectsPtr(graph->Neighbors(message_oid, cache->post_has_creator_t, gdb::Outgoing))->Any();
              gdb::oid_t person_city = sparksee::utils::ObjectsPtr(graph->Neighbors(creator_oid, cache->is_located_in_t, gdb::Outgoing))->Any();
              if(!cities_country1->Exists(person_city) && !cities_country2->Exists(person_city)) {
                graph->GetAttribute(message_oid, cache->post_creation_date_t, val);
                if( val.GetTimestamp() >= start_date.GetTimestamp() && val.GetTimestamp() <= end_date.GetTimestamp() )
                  person_message_count2[creator_oid]++;
              }
            }

            sparksee::utils::ObjectsIteratorPtr iter_comments_2(filtered_comments_2->Iterator()); 
            while(iter_comments_2->HasNext()) {
              gdb::oid_t message_oid = iter_comments_2->Next();
              gdb::oid_t creator_oid = sparksee::utils::ObjectsPtr(graph->Neighbors(message_oid, cache->comment_has_creator_t, gdb::Outgoing))->Any();
              gdb::oid_t person_city = sparksee::utils::ObjectsPtr(graph->Neighbors(creator_oid, cache->is_located_in_t, gdb::Outgoing))->Any();
              if(!cities_country1->Exists(person_city) && !cities_country2->Exists(person_city)) {
                graph->GetAttribute(message_oid, cache->comment_creation_date_t, val);
                if( val.GetTimestamp() >= start_date.GetTimestamp() && val.GetTimestamp() <= end_date.GetTimestamp() )
                  person_message_count2[creator_oid]++;
              }
            }

            timeval endCore;
            gettimeofday(&endCore,NULL);

            sparksee::utils::ObjectsPtr persons(sess->NewObjects());
            for(std::map<gdb::oid_t, unsigned long>::iterator it = person_message_count1.begin(); it != person_message_count1.end(); ++it) {
              persons->Add(it->first);
            }

            for(std::map<gdb::oid_t, unsigned long>::iterator it = person_message_count2.begin(); it != person_message_count2.end(); ++it) {
              persons->Add(it->first);
            }

            sparksee::utils::ObjectsPtr friends(sparksee::utils::k_hop(*sess, *graph, person_oid, cache->knows_t, gdb::Outgoing, 2, false));
            persons->Intersection(friends.get());

            sparksee::utils::ObjectsIteratorPtr iter_persons(persons->Iterator());
            while(iter_persons->HasNext()) {
              gdb::oid_t person_oid = iter_persons->Next();
              Result res;
              graph->GetAttribute(person_oid, cache->person_id_t, val);
              res.person_oid = person_oid;
              res.person_id = val.GetLong();
              res.num_messagesX = person_message_count1[person_oid];
              res.num_messagesY = person_message_count2[person_oid];
              if(res.num_messagesX != 0 && res.num_messagesY != 0) {
                intermediate_result.push_back(res);
              }
            }

            std::sort(intermediate_result.begin(), intermediate_result.end(), compare_result);
            ptree::ptree pt = Project(*graph, *cache, intermediate_result, limit);
            COMMIT_TRANSACTION
#ifdef VERBOSE
            printf("EXIT QUERY3: %lld %s %s %u\n", person_id, country1, country2, limit);
            /*timeval end;
            gettimeofday(&end,NULL);
            unsigned long executionTime = (end.tv_sec - start.tv_sec)*1000 + (end.tv_usec - start.tv_usec)/1000;
            unsigned long executionTimeCore = (endCore.tv_sec - startCore.tv_sec)*1000 + (endCore.tv_usec - startCore.tv_usec)/1000;
            printf("QUERY3:EXECUTION_TIME: %lu %lu\n",executionTime, executionTimeCore);
            */
#endif
            char* res = snb::utils::to_json(pt);
            return datatypes::Buffer(res, strlen(res));
            END_EXCEPTION
        }
    }
}


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
namespace query12 {

        struct Result {
            gdb::Objects* comments;
            long long person_id;
            gdb::oid_t person_oid;
        };

        bool compare_result(const Result &res_a, const Result &res_b) {
            long countA = res_a.comments != NULL ? res_a.comments->Count() : 0;
            long countB = res_b.comments != NULL ? res_b.comments->Count() : 0;
            if( countA != countB ) {
                return countA > countB;
            }
            return res_a.person_id < res_b.person_id;
        }

        static ptree::ptree Project(gdb::Session& sess, const gdb::Graph &graph,
                const snb::TypeCache &cache, const Result &result, gdb::Objects* filtered_tags) {
            gdb::Value val;
            ptree::ptree pt;
            gdb::Graph& nc_graph = const_cast<gdb::Graph&>(graph);
            nc_graph.GetAttribute(result.person_oid, cache.person_id_t, val);
            pt.put<long long>("Person.id", val.GetLong());
            nc_graph.GetAttribute(result.person_oid, cache.person_first_name_t, val);
            pt.put<std::string>("Person.firstName", sparksee::utils::to_string(val.GetString()));
            nc_graph.GetAttribute(result.person_oid, cache.person_last_name_t, val);
            pt.put<std::string>("Person.lastName", sparksee::utils::to_string(val.GetString()));
            gdb::Objects* tags = sess.NewObjects();
            if( result.comments ) {
                gdb::ObjectsIterator* iter_comments = result.comments->Iterator();
                while(iter_comments->HasNext()) {
                    gdb::oid_t comment_oid = iter_comments->Next();
                    gdb::Objects* posts = nc_graph.Neighbors(comment_oid, cache.reply_of_t, gdb::Outgoing);
                    gdb::Objects* post_tags = nc_graph.Neighbors(posts->Any(), cache.has_tag_t, gdb::Outgoing);
                    tags->Union(post_tags);
                    delete post_tags;
                    delete posts;
                }
                delete iter_comments;
            }

            ptree::ptree tags_pt;
            gdb::ObjectsIterator* iter_tags = tags->Iterator();
            while (iter_tags->HasNext()) {
                gdb::oid_t tag = iter_tags->Next();
                if( filtered_tags->Exists(tag) ) {
                  nc_graph.GetAttribute(tag, cache.tag_name_t, val);
                  tags_pt.push_back( std::make_pair("", sparksee::utils::to_string(val.GetString())));
                }
            }
            delete iter_tags;
            delete tags;
            pt.push_back(std::make_pair("Tags", tags_pt));
            pt.put<int>("count", static_cast<int>(result.comments ? result.comments->Count() : 0));
            return pt;
        }

        static ptree::ptree Project(gdb::Session& sess, const gdb::Graph &graph,
                const snb::TypeCache &cache,
                const std::vector<Result> &results, int limit, gdb::Objects* tags ) {
            ptree::ptree pt;
            int counter = 0;
            for (std::vector<Result>::const_iterator it = results.begin();
                    it != results.end() && counter < limit; ++it, ++counter) {
                pt.push_back(std::make_pair("", Project(sess,graph, cache, *it, tags)));
            }
            return pt;
        }
 
        datatypes::Buffer Execute(gdb::Session *sess, long long person_id,
                const char *tag_class, unsigned int limit) {
#ifdef VERBOSE
            printf("QUERY12: %lli %s %u\n", person_id, tag_class, limit);
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


            gdb::oid_t person_oid = graph->FindObject(cache->person_id_t, val);

            gdb::Objects* tags = sess->NewObjects();
            val.SetString(sparksee::utils::to_wstring(tag_class));
            gdb::oid_t tag_class_oid = graph->FindObject(cache->tagclass_name_t, val);
            std::list<gdb::oid_t> tag_classes;
            tag_classes.push_back(tag_class_oid);
            while(!tag_classes.empty()){
                gdb::oid_t next_tag_class = tag_classes.front();
                tag_classes.pop_front();
                gdb::Objects* current_tags = graph->Neighbors(next_tag_class, cache->has_type_t, gdb::Ingoing);
                gdb::ObjectsIterator* iter_tags = current_tags->Iterator();
                while(iter_tags->HasNext()) {
                    tags->Add(iter_tags->Next());
                }
                delete iter_tags;
                delete current_tags;

                gdb::Objects* sub_classes = graph->Neighbors( next_tag_class, cache->is_subclass_of_t, gdb::Ingoing);
                gdb::ObjectsIterator* iter_classes = sub_classes->Iterator();
                while(iter_classes->HasNext()) {
                    tag_classes.push_back(iter_classes->Next());
                }
                delete iter_classes;
                delete sub_classes;
            }

            timeval startCore;
            gettimeofday(&startCore,NULL);

            gdb::Objects* message_tags = graph->Neighbors(tags, cache->post_has_tag_t, gdb::Ingoing);
            gdb::Objects* friends = graph->Neighbors(person_oid, cache->knows_t, gdb::Outgoing );
            gdb::Objects* comments = graph->Neighbors(friends, cache->comment_has_creator_t, gdb::Ingoing);
            gdb::Objects* filtered_comments = sess->NewObjects();
            gdb::ObjectsIterator* iter_comments = comments->Iterator();
            while(iter_comments->HasNext()) {
                gdb::oid_t comment_oid = iter_comments->Next();
                gdb::Objects* replies = graph->Neighbors(comment_oid, cache->reply_of_t, gdb::Outgoing);
                gdb::oid_t message_oid = replies->Any();
                delete replies;
                if(message_tags->Exists(message_oid)){
                  filtered_comments->Add(comment_oid);
                }
            }
            delete message_tags;
            delete iter_comments;
            delete comments;

            gdb::Objects* selected_friends = sess->NewObjects();
            gdb::Objects* creators = graph->Explode(filtered_comments, cache->comment_has_creator_t, gdb::Outgoing);

            timeval endCore;
            gettimeofday(&endCore,NULL);

            sparksee::utils::GroupBy* groups = sparksee::utils::group_by(*sess,*graph,creators, sparksee::utils::GroupBy::kHead);
            sparksee::utils::GroupByIterator* iter_groups = groups->iterator();
            while(iter_groups->has_next()) {
              sparksee::utils::Group group = iter_groups->next();
                gdb::oid_t friend_oid = group.key();
                selected_friends->Add(friend_oid);
                graph->GetAttribute(friend_oid, cache->person_id_t, val);
                Result res = {group.group(), val.GetLong(), friend_oid};
                intermediate_result.push_back(res);
            }
            delete iter_groups;
            delete creators;
            delete filtered_comments;

            delete selected_friends;
            delete friends;
            std::sort(intermediate_result.begin(), intermediate_result.end(),
                    compare_result);
            ptree::ptree pt = Project(*sess,*graph, *cache, intermediate_result, limit, tags);
            delete tags;
            delete groups;
            COMMIT_TRANSACTION
            delete graph;
#ifdef VERBOSE
            printf("EXIT QUERY12: %lli %s %u\n", person_id, tag_class, limit);
            /*
            timeval end;
            gettimeofday(&end,NULL);
            unsigned long executionTime = (end.tv_sec - start.tv_sec)*1000 + (end.tv_usec - start.tv_usec)/1000;
            unsigned long executionTimeCore = (endCore.tv_sec - startCore.tv_sec)*1000 + (endCore.tv_usec - startCore.tv_usec)/1000;
            printf("QUERY12:EXECUTION_TIME: %lu %lu\n",executionTime, executionTimeCore);
            */
#endif
            char* res = snb::utils::to_json(pt);
            return datatypes::Buffer(res, strlen(res));
            END_EXCEPTION
        }
}
}

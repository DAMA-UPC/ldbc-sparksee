
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
    namespace update7{

        namespace gdb = sparksee::gdb;
        namespace snb = sparksee::snb;
        namespace ptree = boost::property_tree;

        datatypes::Buffer Execute(gdb::Session *sess, long long comment_id,  long long creation_date, const char* location_ip,
                const char* browser_used,  const char* content, int length, long long creator_id, long long location_id,
                long long reply_of_post_id, long long reply_of_comment_id, int num_tags, const long long tags[] ) {
#ifdef VERBOSE
            printf("UPDATE QUERY7: comment %lli in replyof post %lli or comment %lli\n", comment_id, reply_of_post_id, reply_of_comment_id);
#endif

            BEGIN_EXCEPTION
            gdb::Graph *graph = sess->GetGraph();
            snb::TypeCache *cache = snb::TypeCache::instance(graph);
            gdb::Value val;
            BEGIN_UPDATE_TRANSACTION

            gdb::oid_t comment_oid = graph->NewNode(cache->comment_t);
            val.SetLong(comment_id);
            graph->SetAttribute(comment_oid, cache->comment_id_t, val);

            val.SetTimestamp(creation_date);
            graph->SetAttribute(comment_oid, cache->comment_creation_date_t, val);
            val.SetString(sparksee::utils::to_wstring(location_ip));
            graph->SetAttribute(comment_oid, cache->comment_locationIP_t, val);
            val.SetString(sparksee::utils::to_wstring(browser_used));
            graph->SetAttribute(comment_oid, cache->comment_browser_used_t, val);
            std::string content_str(content);
            val.SetString(sparksee::utils::to_wstring(content_str));
            graph->SetAttribute(comment_oid, cache->comment_content_t, val);
            val.SetInteger(length);
            graph->SetAttribute(comment_oid, cache->comment_length_t, val);
            val.SetLong(creator_id);
            gdb::oid_t creator_oid = graph->FindObject(cache->person_id_t, val);
            graph->NewEdge(cache->comment_has_creator_t, comment_oid, creator_oid);
            val.SetLong(location_id);
            gdb::oid_t location_oid = graph->FindObject(cache->place_id_t, val);
            graph->NewEdge(cache->is_located_in_t, comment_oid, location_oid);
            
            gdb::oid_t message_creator;
            for( int i = 0; i < num_tags; ++i ) {
                val.SetLong(tags[i]);
                gdb::oid_t tag_oid = graph->FindObject(cache->tag_id_t, val);
                graph->NewEdge(cache->has_tag_t, comment_oid, tag_oid);
            }
            gdb::oid_t message_oid; 
            bool post = false;
            if( reply_of_post_id != -1 ) {
                val.SetLong(reply_of_post_id);
                message_oid = graph->FindObject(cache->post_id_t, val);
                gdb::Objects* aux = graph->Neighbors(message_oid, cache->post_has_creator_t, gdb::Outgoing);
                message_creator = aux->Any();
                delete aux;
                post = true;
            } else {
                val.SetLong(reply_of_comment_id);
                message_oid = graph->FindObject(cache->comment_id_t, val);
                gdb::Objects* aux = graph->Neighbors(message_oid, cache->comment_has_creator_t, gdb::Outgoing);
                message_creator = aux->Any();
                delete aux;
            }
            graph->NewEdge(cache->reply_of_t, comment_oid, message_oid);

            //Updating similarity if necessary

            gdb::oid_t edge = graph->FindEdge(cache->knows_t,message_creator,creator_oid);
            if(edge != gdb::Objects::InvalidOID) {
              graph->GetAttribute(edge,cache->knows_similarity_t, val);
              val.SetDouble(val.GetDouble() + (post ? 1.0 : 0.5));
              graph->SetAttribute(edge,cache->knows_similarity_t, val);
            }


            COMMIT_TRANSACTION
            delete graph;
            char* res = snb::utils::empty_json();

#ifdef VERBOSE
            printf("EXIT UPDATE QUERY7: comment %lli in replyof post %lli or comment %lli\n", comment_id, reply_of_post_id, reply_of_comment_id);
#endif
            return datatypes::Buffer(res,strlen(res));
            END_EXCEPTION
        }
    }
}

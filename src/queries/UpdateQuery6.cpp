
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
    namespace update6{

        namespace gdb = sparksee::gdb;
        namespace snb = sparksee::snb;
        namespace ptree = boost::property_tree;

        datatypes::Buffer Execute(gdb::Session *sess, long long post_id, const char* image_file, long long creation_date, const char* location_ip,
                const char* browser_used, const char* language, const char* content, int length, long long creator_id, long long forum_id, long long location_id, 
               int num_tags, const long long tags[] ) {
#ifdef VERBOSE
            printf("UPDATE QUERY6: post %lli in forum %lli\n",post_id, forum_id);
#endif

            BEGIN_EXCEPTION
            gdb::Graph *graph = sess->GetGraph();
            snb::TypeCache *cache = snb::TypeCache::instance(graph);
            gdb::Value val;
            BEGIN_UPDATE_TRANSACTION
            gdb::oid_t post_oid = graph->NewNode(cache->post_t);
            val.SetLong(post_id);
            graph->SetAttribute(post_oid, cache->post_id_t, val);
            std::string image_str(image_file);
            if( image_str.length() > 0 ) {
                val.SetString(sparksee::utils::to_wstring(image_str));
                graph->SetAttribute(post_oid, cache->post_image_file_t, val);
            }
            val.SetTimestamp(creation_date);
            graph->SetAttribute(post_oid, cache->post_creation_date_t, val);
            val.SetString(sparksee::utils::to_wstring(location_ip));
            graph->SetAttribute(post_oid, cache->post_locationIP_t, val);
            val.SetString(sparksee::utils::to_wstring(browser_used));
            graph->SetAttribute(post_oid, cache->post_browser_used_t, val);
            val.SetString(sparksee::utils::to_wstring(language));
            graph->SetAttribute(post_oid, cache->post_language_t, val);
            if( strlen(content) > 0 ) {
                val.SetString(sparksee::utils::to_wstring(content));
                graph->SetAttribute(post_oid, cache->post_content_t, val);
                val.SetInteger(length);
            } else {
                val.SetInteger(0);
            }
            graph->SetAttribute(post_oid, cache->post_length_t, val);
            val.SetLong(creator_id);
            gdb::oid_t creator_oid = graph->FindObject(cache->person_id_t, val);
            graph->NewEdge(cache->post_has_creator_t, post_oid, creator_oid);
            val.SetLong(forum_id);
            gdb::oid_t forum_oid = graph->FindObject(cache->forum_id_t, val );
            graph->NewEdge(cache->container_of_t, forum_oid, post_oid );
            gdb::oid_t edge_oid = graph->FindEdge(cache->has_member_with_posts_t, forum_oid, creator_oid);
            if( edge_oid == gdb::Objects::InvalidOID ) {
                edge_oid = graph->FindEdge(cache->has_member_t, forum_oid, creator_oid);
                if( edge_oid != gdb::Objects::InvalidOID ) {
                    graph->GetAttribute(edge_oid, cache->has_member_join_date_t, val);
                    edge_oid = graph->NewEdge(cache->has_member_with_posts_t, forum_oid, creator_oid);
                    graph->SetAttribute(edge_oid, cache->has_member_with_posts_join_date_t,val);
                }
            }

            val.SetLong(location_id);
            gdb::oid_t location_oid = graph->FindObject(cache->place_id_t, val);
            graph->NewEdge(cache->is_located_in_t, post_oid, location_oid);

            for( int i = 0; i < num_tags; ++i ) {
                val.SetLong(tags[i]);
                gdb::oid_t tag_oid = graph->FindObject(cache->tag_id_t, val);
                graph->NewEdge(cache->has_tag_t, post_oid, tag_oid);
            }
            COMMIT_TRANSACTION
            delete graph;
            char* res = snb::utils::empty_json();

#ifdef VERBOSE
            printf("EXIT UPDATE QUERY6: post %lli in forum %lli\n",post_id, forum_id);
#endif
            return datatypes::Buffer(res,strlen(res));
            END_EXCEPTION
        }
    }
}

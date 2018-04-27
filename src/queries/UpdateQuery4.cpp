
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
    namespace update4{

        namespace gdb = sparksee::gdb;
        namespace snb = sparksee::snb;
        namespace ptree = boost::property_tree;

        datatypes::Buffer Execute(gdb::Session *sess, long long forum_id, const char* title, long long creation_date, long long moderator_id, int num_tags, long long tags[]) {
#ifdef VERBOSE
            printf("UPDATE QUERY4: ADD FORUM %lli of moderator %lli \n", forum_id, moderator_id);
#endif
            BEGIN_EXCEPTION
            gdb::Graph *graph = sess->GetGraph();
            snb::TypeCache *cache = snb::TypeCache::instance(graph);
            gdb::Value val;
            BEGIN_UPDATE_TRANSACTION
            gdb::oid_t forum_oid = graph->NewNode(cache->forum_t);
            val.SetLong(forum_id);
            graph->SetAttribute(forum_oid, cache->forum_id_t, val);
            val.SetString(sparksee::utils::to_wstring(title));
            graph->SetAttribute(forum_oid, cache->forum_title_t, val);
            val.SetTimestamp(creation_date);
            graph->SetAttribute(forum_oid, cache->forum_creation_date_t, val);
            val.SetLong(moderator_id);
            gdb::oid_t moderator_oid = graph->FindObject(cache->person_id_t, val);
            graph->NewEdge(cache->has_moderator_t, forum_oid, moderator_oid);
            for( int i = 0; i < num_tags; ++i ){
                val.SetLong(tags[i]);
                gdb::oid_t tag_oid = graph->FindObject(cache->tag_id_t, val);
                graph->NewEdge(cache->has_tag_t, forum_oid, tag_oid);
            }
            COMMIT_TRANSACTION
            delete graph;
            char* res = snb::utils::empty_json();

#ifdef VERBOSE
            printf("EXIT UPDATE QUERY4: ADD FORUM %lli of moderator %lli \n", forum_id, moderator_id);
#endif
            return datatypes::Buffer(res,strlen(res));
            END_EXCEPTION
        }
    }
}

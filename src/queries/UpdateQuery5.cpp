
#include "snbInteractive.h"
#include "Database.h"
#include "TypeCache.h"
#include "Utils.h"

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
    namespace update5{

        namespace gdb = sparksee::gdb;
        namespace snb = sparksee::snb;
        namespace ptree = boost::property_tree;

        datatypes::Buffer Execute(gdb::Session *sess, long long person_id, long long forum_id, long long join_date) {
#ifdef VERBOSE
            printf("UPDATE QUERY5: %llu %llu %llu\n", person_id, forum_id, join_date);
#endif
            BEGIN_EXCEPTION
            gdb::Graph *graph = sess->GetGraph();
            snb::TypeCache *cache = snb::TypeCache::instance(graph);
            gdb::Value val;
            val.SetLong(person_id);
            BEGIN_UPDATE_TRANSACTION
            gdb::oid_t person_oid = graph->FindObject(cache->person_id_t, val);
            val.SetLong(forum_id);
            gdb::oid_t forum_oid = graph->FindObject(cache->forum_id_t, val);
            gdb::oid_t edge_oid = graph->NewEdge(cache->has_member_t, forum_oid, person_oid);
            val.SetTimestamp(join_date);
            graph->SetAttribute(edge_oid, cache->has_member_join_date_t, val);
            COMMIT_TRANSACTION
            delete graph;
            char* res = snb::utils::empty_json();

#ifdef VERBOSE
            printf("EXIT UPDATE QUERY5: %llu %llu %llu\n", person_id, forum_id, join_date);
#endif
            return datatypes::Buffer(res,strlen(res));
            END_EXCEPTION
        }
    }
}

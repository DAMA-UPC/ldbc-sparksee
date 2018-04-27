
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
    namespace update3{

        namespace gdb = sparksee::gdb;
        namespace snb = sparksee::snb;
        namespace ptree = boost::property_tree;

        datatypes::Buffer Execute(gdb::Session *sess, long long person_id, long long comment_id, long long creation_date) {
#ifdef VERBOSE
            printf("UPDATE QUERY3: %lli likes comment %lli\n",person_id, comment_id);
#endif

            BEGIN_EXCEPTION
                gdb::Graph *graph = sess->GetGraph();
                snb::TypeCache *cache = snb::TypeCache::instance(graph);
                BEGIN_UPDATE_TRANSACTION
                gdb::Value val;
                val.SetLong(person_id);
                gdb::oid_t person_oid = graph->FindObject(cache->person_id_t, val);
                val.SetLong(comment_id);
                gdb::oid_t comment_oid = graph->FindObject(cache->comment_id_t, val);
                gdb::oid_t edge_oid = graph->NewEdge(cache->likes_t, person_oid, comment_oid);
                val.SetTimestamp(creation_date);
                graph->SetAttribute(edge_oid, cache->likes_creation_date_t, val);
                COMMIT_TRANSACTION
                delete graph;
                char* res = snb::utils::empty_json();
                return datatypes::Buffer(res,strlen(res));
                END_EXCEPTION
#ifdef VERBOSE
            printf("EXIT UPDATE QUERY3: %lli likes comment %lli\n",person_id, comment_id);
#endif
        }
    }
}

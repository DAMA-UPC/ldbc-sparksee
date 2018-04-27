
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
    namespace update8{

        namespace gdb = sparksee::gdb;
        namespace snb = sparksee::snb;
        namespace ptree = boost::property_tree;

        datatypes::Buffer Execute(gdb::Session *sess, long long person1_id, long long person2_id, long long creation_date) {
#ifdef VERBOSE
            printf("UPDATE QUERY8: ADD FRIENDSHIP %llu %llu\n", person1_id, person2_id );
#endif
            BEGIN_EXCEPTION
            gdb::Graph *graph = sess->GetGraph();
            snb::TypeCache *cache = snb::TypeCache::instance(graph);
            gdb::Value val;
            BEGIN_UPDATE_TRANSACTION

            val.SetLong(person1_id);
            gdb::oid_t person1_oid = graph->FindObject(cache->person_id_t, val);

            val.SetLong(person2_id);
            gdb::oid_t person2_oid = graph->FindObject(cache->person_id_t, val);

            gdb::oid_t edge_oid = graph->NewEdge(cache->knows_t, person1_oid, person2_oid);
            val.SetTimestamp(creation_date);
            graph->SetAttribute(edge_oid, cache->knows_creation_date_t, val);

            val.SetDouble(snb::utils::similarity(*sess,*graph,*cache,person1_oid,person2_oid));
            graph->SetAttribute(edge_oid, cache->knows_similarity_t,val);
            COMMIT_TRANSACTION
            delete graph;
            char* res = snb::utils::empty_json();

#ifdef VERBOSE
            printf("EXIT UPDATE QUERY8: ADD FRIENDSHIP %llu %llu\n", person1_id, person2_id );
#endif
            return datatypes::Buffer(res,strlen(res));
            END_EXCEPTION
        }
    }
}

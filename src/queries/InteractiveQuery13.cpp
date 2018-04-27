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
#include <algorithms/SinglePairShortestPathBFS.h>
#include <algorithm>
#include <stdio.h>
#include <boostPatch/property_tree/ptree.hpp>
#include <boostPatch/property_tree/json_parser.hpp>
#include <boost/format.hpp>

namespace gdb = sparksee::gdb;
namespace algorithms = sparksee::algorithms;
namespace snb = sparksee::snb;
namespace ptree = boost::property_tree;

namespace interactive {
namespace query13 {


        datatypes::Buffer Execute(gdb::Session *sess, long long person1, long long person2) {
#ifdef VERBOSE
            printf("QUERY13: %lli %lli\n", person1, person2);
#endif

            BEGIN_EXCEPTION
            gdb::Graph *graph = sess->GetGraph();
            snb::TypeCache *cache = snb::TypeCache::instance(graph);
            gdb::Value val;

            val.SetLong(person1);
            BEGIN_TRANSACTION
            gdb::oid_t person1_oid = graph->FindObject(cache->person_id_t, val);
            val.SetLong(person2);
            gdb::oid_t person2_oid = graph->FindObject(cache->person_id_t, val);
            algorithms::SinglePairShortestPathBFS* bfs = new algorithms::SinglePairShortestPathBFS(*sess, person1_oid, person2_oid);
            bfs->AddNodeType(cache->person_t);
            bfs->AddEdgeType(cache->knows_t, gdb::Any);
            bfs->CheckOnlyExistence();
            bfs->Run();
            long length = -1;
            if(bfs->Exists()) {
                length = bfs->GetCost();
            }
            
            boost::format frmt("{\"\":[{\"length\":\"%d\"}]}");
            frmt % length ;
            COMMIT_TRANSACTION
            delete bfs;
            delete graph;
            std::string ret_str = frmt.str();
            char* result = new char[ret_str.length()+1];
            std::strcpy(result, ret_str.c_str()); 
#ifdef VERBOSE
            printf("EXIT QUERY13: %lli %lli\n", person1, person2);
#endif
            return datatypes::Buffer(result, strlen(result));
            END_EXCEPTION
        }
}
}

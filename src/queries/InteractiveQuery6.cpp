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
#include <time.h>

namespace gdb = sparksee::gdb;
namespace snb = sparksee::snb;
namespace ptree = boost::property_tree;

namespace interactive {
namespace query6 {

    struct Result {
        long count;
        std::string name;
    };

    bool compare_result(const Result &res_a, const Result &res_b) {
        if (res_a.count != res_b.count )  
            return res_a.count > res_b.count;
        return res_a.name < res_b.name;
    }

        static ptree::ptree Project(const gdb::Graph &graph,
                const snb::TypeCache &cache, const Result &result) {
            gdb::Value val;
            ptree::ptree pt;
            pt.put<std::string>("Tag.name", result.name);
            pt.put<long long>("count", result.count );
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
                    const char *tag, unsigned int limit) {
#ifdef VERBOSE
    printf("QUERY6: %lli %s %u\n", person_id, tag, limit);
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

            timeval startCore;
            gettimeofday(&startCore,NULL);
    gdb::oid_t person_oid = graph->FindObject(cache->person_id_t, val);
    val.SetString( sparksee::utils::to_wstring(tag));
    gdb::oid_t tag_oid = graph->FindObject(cache->tag_name_t, val);

    if( tag_oid == gdb::Objects::InvalidOID ) {
        ptree::ptree pt;
        COMMIT_TRANSACTION
        delete graph;
        char * res = snb::utils::to_json(pt);
        return datatypes::Buffer(res, strlen(res));
    }

    gdb::Objects* posts_with_tag = graph->Neighbors(tag_oid, cache->has_tag_t, gdb::Ingoing);

    std::map<gdb::oid_t, int> tag_count;
    gdb::Objects* friends = sparksee::utils::k_hop(*sess, *graph, person_oid, cache->knows_t, gdb::Outgoing, 2, false );
    gdb::ObjectsIterator* iter_friends = friends->Iterator();
    while(iter_friends->HasNext()) {
        gdb::oid_t friend_oid = iter_friends->Next();
        gdb::Objects* friend_posts = graph->Neighbors(friend_oid, cache->post_has_creator_t, gdb::Ingoing);
        gdb::Objects* post_tags = gdb::Objects::CombineIntersection(friend_posts, posts_with_tag);
        gdb::ObjectsIterator* iter_posts = post_tags->Iterator();
        while(iter_posts->HasNext()) {
            gdb::oid_t post_oid = iter_posts->Next();
            gdb::Objects* tags = graph->Neighbors(post_oid, cache->has_tag_t, gdb::Outgoing);
            gdb::ObjectsIterator* iter_tags = tags->Iterator();
            while(iter_tags->HasNext()){
                gdb::oid_t tag = iter_tags->Next();
                std::pair<std::map<gdb::oid_t, int>::iterator, bool > current = tag_count.insert(std::pair<gdb::oid_t, int>(tag,0)); 
                current.first->second++;
            }
            delete iter_tags;
            delete tags;
        }
        delete iter_posts;
        delete post_tags;
        delete friend_posts;
    }
    delete posts_with_tag;

            timeval endCore;
            gettimeofday(&endCore,NULL);

    for( std::map<gdb::oid_t, int>::iterator it = tag_count.begin(); it != tag_count.end(); ++it ) {
        if( it->first != tag_oid ) {
            graph->GetAttribute(it->first, cache->tag_name_t, val);
            Result res = {it->second, sparksee::utils::to_string(val.GetString())};
            intermediate_result.push_back(res);
        }
    }


    std::sort(intermediate_result.begin(), intermediate_result.end(), compare_result);
    ptree::ptree pt = Project(*graph, *cache, intermediate_result, limit);
    COMMIT_TRANSACTION
    delete iter_friends;
    delete friends;
    delete graph;
#ifdef VERBOSE
    printf("EXIT QUERY6: %lli %s %u\n", person_id, tag, limit);
            /*timeval end;
            gettimeofday(&end,NULL);
            unsigned long executionTime = (end.tv_sec - start.tv_sec)*1000 + (end.tv_usec - start.tv_usec)/1000;
            unsigned long executionTimeCore = (endCore.tv_sec - startCore.tv_sec)*1000 + (endCore.tv_usec - startCore.tv_usec)/1000;
            printf("QUERY6:EXECUTION_TIME: %lu %lu\n",executionTime, executionTimeCore);
            */
#endif
    char* res = snb::utils::to_json(pt);
    return datatypes::Buffer(res, strlen(res));
    END_EXCEPTION
}
}
}

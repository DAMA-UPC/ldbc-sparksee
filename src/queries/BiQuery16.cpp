#include "Database.h"
#include "TypeCache.h"
#include "Utils.h"
#include <utils/Utils.h>
#include "snbInteractive.h"
#include <gdb/Graph.h>
#include <gdb/Objects.h>
#include <gdb/ObjectsIterator.h>
#include <gdb/Session.h>
#include <gdb/Sparksee.h>
#include <gdb/Value.h>
#include <stdio.h>
#include <string>
#include <cstring>
#include <vector>
#include <map>
#include <queue>
#include <algorithm>
#include <boostPatch/property_tree/ptree.hpp>
#include <boostPatch/property_tree/json_parser.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/bind.hpp>
#include <time.h>

namespace gdb = sparksee::gdb;
namespace snb = sparksee::snb;
namespace ptree = boost::property_tree;

namespace bi {
namespace query16 {

struct Result {

  Result(long long id, std::string name, int count)
    : person_id(id), message_count(count), tag_name(name) {}

  long long person_id;
  int message_count;
  std::string tag_name;

  bool operator<(const Result &res_b) const {
    if (message_count != res_b.message_count) {
      return sparksee::utils::descending(message_count, res_b.message_count);
    }
    if( tag_name != res_b.tag_name ) {
      return sparksee::utils::ascending(tag_name, res_b.tag_name);
    }
    return sparksee::utils::ascending(person_id, res_b.person_id);
  }

};

struct compare_priority {
  bool operator()(const Result &lhs, const Result &rhs) {
    /*if(lhs.message_count != rhs.message_count) {
      return sparksee::utils::ascending(lhs.message_count, rhs.message_count);
    }
    if(lhs.tag_name != rhs.tag_name) {
      return sparksee::utils::descending(lhs.tag_name, rhs.tag_name);
    }
    return sparksee::utils::descending(lhs.person_id, rhs.person_id);
    */
    return lhs < rhs;
  }
};

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Result &result) {
  gdb::Value val;
  ptree::ptree pt;

  pt.put<long long>("Person.id", result.person_id);
  pt.put<std::string>("Tag.name", result.tag_name);
  pt.put<int>("count", result.message_count);

  return pt;
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache,
                            const std::vector<Result> &results) {
  ptree::ptree pt;
  for (std::vector<Result>::const_iterator it = results.begin();
       it != results.end(); ++it) {
    pt.push_back(std::make_pair("", Project(graph, cache, *it)));
  }
  return pt;
}

/*static void find_distance_ei( gdb::oid_t node, 
                              gdb::Session* sess,
                              gdb::Graph* graph,
                              snb::TypeCache* cache,
                              int min, 
                              int max, 
                              int step,
                              gdb::Objects* visited_edges, 
                              gdb::Objects* found_nodes ) {

  boost::scoped_ptr<gdb::Objects> explode(graph->Explode(node,cache->knows_t, gdb::Any));
  boost::scoped_ptr<gdb::ObjectsIterator> edges(explode->Iterator());
  while(edges->HasNext()) {
    gdb::oid_t next_edge = edges->Next();
    if(!visited_edges->Exists(next_edge)) {
      visited_edges->Add(next_edge);
      gdb::oid_t neighbor = graph->GetEdgePeer(next_edge,node);
      if((step >= min) && (step <= max)) {
        found_nodes->Add(neighbor);
      }
      if(step < max) {
        find_distance_ei(neighbor,
                         sess,
                         graph,
                         cache,
                         min,
                         max,step+1,
                         visited_edges,
                         found_nodes);
      }
      visited_edges->Remove(next_edge);
    }

  }

}*/

static bool is_reachable( gdb::Session* sess,
                          gdb::Graph* graph,
                          snb::TypeCache* cache,
                          int min, 
                          int max,
                          int step,
                          gdb::oid_t start,
                          gdb::oid_t end,
                          gdb::Objects* found_nodes,
                          gdb::Objects* visited_edges) {

  boost::scoped_ptr<gdb::Objects> explode(graph->Explode(start,cache->knows_t, gdb::Any));
  boost::scoped_ptr<gdb::ObjectsIterator> edges(explode->Iterator());
  bool found = false;
  while(edges->HasNext()) {
    gdb::oid_t next_edge = edges->Next();
    if(!visited_edges->Exists(next_edge)) {
      visited_edges->Add(next_edge);
      gdb::oid_t neighbor = graph->GetEdgePeer(next_edge,start);
      if((step >= min) && (step <= max)) {
        found_nodes->Add(neighbor);
        if(neighbor == end) {
          found = true;
          break;
        }
      } 
      if(step < max) {
        if(is_reachable(sess,
                         graph,
                         cache,
                         min,
                         max,
                         step+1,
                         neighbor,
                         end,
                         found_nodes,
                         visited_edges)) {
          found = true;
          break;
        }
      }
      visited_edges->Remove(next_edge);
    }
  }
  return found;
}

static bool is_reachable(gdb::Session* sess,
                          gdb::Graph* graph,
                          snb::TypeCache* cache,
                          int min, 
                          int max,
                          gdb::oid_t start,
                          gdb::oid_t end,
                          gdb::Objects* found_nodes) {

  for(int i = min;  i < max; ++i ) {
    boost::scoped_ptr<gdb::Objects> visited_edges(sess->NewObjects());
    if(is_reachable(sess,graph,cache,min,i,1,start, end,found_nodes, visited_edges.get())) {
      return true;
    }
  }
  return false;

}

struct PersonNumMessages{
  gdb::oid_t person_oid;
  long long person_id;
  long long num_messages;
   bool operator<(const PersonNumMessages& b) const {
     if(num_messages != b.num_messages)
        return sparksee::utils::descending(num_messages,b.num_messages);
     return sparksee::utils::ascending(person_id,b.person_id);
    }
};

datatypes::Buffer Execute(gdb::Session *sess, long long person_id, const char* country_name, const char* tagclass_name, int min, int max, int limit) {
#ifdef VERBOSE
  printf("Bi QUERY16: %llu %s %s %d %d \n", person_id, country_name, tagclass_name, min, max);
#endif

  BEGIN_EXCEPTION

  boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
  snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

  BEGIN_TRANSACTION;

  gdb::Value value;

  long long country_oid = graph->FindObject(cache->place_name_t, value.SetString(sparksee::utils::to_wstring(country_name)));
  boost::scoped_ptr<gdb::Objects> country_persons(snb::utils::people_from_country(country_oid, *graph.get()));

  long long start_person_oid = graph->FindObject(cache->person_id_t, value.SetLong(person_id));
  boost::scoped_ptr<gdb::Objects> persons(graph->Select(cache->person_t));

  persons->Intersection(country_persons.get());
  persons->Remove(start_person_oid);

  long long class_oid = graph->FindObject(cache->tagclass_name_t, value.SetString(sparksee::utils::to_wstring(tagclass_name)));
  boost::scoped_ptr<gdb::Objects> tags(graph->Neighbors(class_oid, cache->has_type_t, gdb::Ingoing));
  {
    sparksee::utils::ObjectsIteratorPtr iter_tags(tags->Iterator());
    while(iter_tags->HasNext()){
      gdb::oid_t tag_oid = iter_tags->Next();
      graph->GetAttribute(tag_oid, cache->tag_name_t, value);
    }
  }
  boost::scoped_ptr<gdb::Objects> messages(graph->Neighbors(tags.get(), cache->has_tag_t, gdb::Ingoing));

  std::vector<PersonNumMessages> candidate_persons;
  candidate_persons.reserve(persons->Count());

  int counter = 0;
  boost::scoped_ptr<gdb::ObjectsIterator> person_iterator(persons->Iterator());
  while (person_iterator->HasNext()) {
    gdb::oid_t person_oid = person_iterator->Next(); 
    PersonNumMessages person_num_messages;
    person_num_messages.person_oid = person_oid;

    graph->GetAttribute(person_oid, cache->person_id_t, value);
    person_num_messages.person_id  = value.GetLong();
    person_num_messages.num_messages = graph->Degree(person_oid, cache->post_has_creator_t, gdb::Ingoing) + 
                             graph->Degree(person_oid, cache->comment_has_creator_t, gdb::Ingoing);

    candidate_persons.push_back(person_num_messages);
    counter++;
  }
  

  std::sort(candidate_persons.begin(), candidate_persons.end());
  std::priority_queue<Result, std::vector<Result>, compare_priority > top_k;

  boost::scoped_ptr<gdb::Objects> reachable_nodes(sess->NewObjects());
  long num_processed = 0;
  for(std::vector<PersonNumMessages>::iterator it_candidates = candidate_persons.begin(); it_candidates != candidate_persons.end(); ++it_candidates) {
    gdb::oid_t person_oid = it_candidates->person_oid;
    long long person_id = it_candidates->person_id;

    if(static_cast<int>(top_k.size()) == limit) {
      const Result& current_limit = top_k.top();
      if(current_limit.message_count > it_candidates->num_messages) {
        break;
      }
    }

    boost::scoped_ptr<gdb::Objects> person_posts(graph->Neighbors(person_oid, cache->post_has_creator_t, gdb::Ingoing));
    boost::scoped_ptr<gdb::Objects> person_comments(graph->Neighbors(person_oid, cache->comment_has_creator_t, gdb::Ingoing));
    sparksee::utils::ObjectsPtr person_messages(gdb::Objects::CombineUnion(person_posts.get(), person_comments.get()));
    person_messages->Intersection(messages.get());

    std::map<std::string,int> tags_map;
    sparksee::utils::ObjectsIteratorPtr iter_messages(person_messages->Iterator());
    while(iter_messages->HasNext()){
      gdb::oid_t message_oid = iter_messages->Next();
      sparksee::utils::ObjectsPtr message_tags(graph->Neighbors(message_oid, cache->has_tag_t, gdb::Outgoing));
      sparksee::utils::ObjectsIteratorPtr iter_tags(message_tags->Iterator());
      while(iter_tags->HasNext()){
        gdb::oid_t tag_oid = iter_tags->Next();
        graph->GetAttribute(tag_oid, cache->tag_name_t, value);
        tags_map[sparksee::utils::to_string(value.GetString())]++;
      }
    } 

    bool reachable = reachable_nodes->Exists(person_oid);
    for(std::map<std::string,int>::iterator it = tags_map.begin(); it != tags_map.end(); ++it) {
      if(static_cast<int>(top_k.size()) == limit) {
        const Result& current_limit = top_k.top();
        Result current_result = Result(person_id, it->first, it->second);
        if(current_result < current_limit) {
          if(reachable || is_reachable(sess,graph.get(), cache, min, max, start_person_oid, person_oid, reachable_nodes.get())) {
            top_k.pop();
            top_k.push(current_result);
            reachable = true;
          } else {
            break;
          }
        } 
      } else {
        if(reachable || is_reachable(sess,graph.get(), cache, min, max, start_person_oid, person_oid, reachable_nodes.get() )) {
          top_k.push(Result(person_id, it->first, it->second));
          reachable = true;
        } else {
          break;
        }
      }
    }
    num_processed++;
  }

  int count = limit;
  std::vector<Result> results;
  while(!top_k.empty() && count > 0) {
    results.push_back(top_k.top());
    top_k.pop();
    count--;
  }
  std::sort(results.begin(), results.end());
  sparksee::utils::top(results, limit);
  ptree::ptree pt = Project(*graph, *cache, results);
  COMMIT_TRANSACTION

#ifdef VERBOSE
    printf("STATS: %lu %lu\n", num_processed, candidate_persons.size() );
  printf("EXIT BI QUERY16: %llu %s %s %lu\n", person_id, country_name, tagclass_name, results.size());
#endif
  char* ret = snb::utils::to_json(pt);
  return datatypes::Buffer(ret, strlen(ret));

  END_EXCEPTION
}
}
}

#include "snbBi.h"
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
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <time.h>

namespace gdb = sparksee::gdb;
namespace snb = sparksee::snb;
namespace ptree = boost::property_tree;


namespace bi {
namespace query25 {

struct Result {
  std::vector<long long> path;
  double weight;
};

bool compare_result(const Result &res_a, const Result &res_b) {
  if(res_a.weight != res_b.weight) {
    return sparksee::utils::descending(res_a.weight,res_b.weight);
  }
  for(int i = 0; i < static_cast<int>(res_a.path.size()); i++) {
    if(res_a.path[i] == res_b.path[i]) {
      continue;
    }
    return sparksee::utils::ascending(res_a.path[i], res_b.path[i]);
  }
  return false;
}

static ptree::ptree Project(gdb::Session& sess, const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Result &result) {
  gdb::Value val;
  ptree::ptree pt;

  ptree::ptree path_pt;
  for( std::vector<long long>::const_iterator it = result.path.begin(); it != result.path.end(); ++it ) {
    path_pt.push_back( std::make_pair("", sparksee::utils::to_string(*it)));
  }
  pt.push_back(std::make_pair("path", path_pt));
  pt.put<double>("weight", result.weight);
  return pt;
}

static ptree::ptree Project(gdb::Session& sess, const gdb::Graph &graph,
                            const snb::TypeCache &cache,
                            const std::vector<Result> &results) {
  ptree::ptree pt;
  for (std::vector<Result>::const_iterator it = results.begin();
       it != results.end(); ++it) {
    pt.push_back(std::make_pair("", Project(sess,graph, cache, *it)));
  }
  return pt;
}

float ComputeWeight(gdb::Session* sess, gdb::Graph* graph, snb::TypeCache* cache, gdb::oid_t person_oid_A, gdb::oid_t person_oid_B, gdb::Objects* messages) {
  boost::scoped_ptr<gdb::Objects> commentsA(graph->Neighbors(person_oid_A, cache->comment_has_creator_t, gdb::Ingoing));
  ///commentsA->Intersection(messages);
  boost::scoped_ptr<gdb::Objects> commentsB(graph->Neighbors(person_oid_B, cache->comment_has_creator_t, gdb::Ingoing));
  //commentsB->Intersection(messages);
  boost::scoped_ptr<gdb::Objects> postsA(graph->Neighbors(person_oid_A, cache->post_has_creator_t, gdb::Ingoing));
  //postsA->Intersection(messages);
  boost::scoped_ptr<gdb::Objects> postsB(graph->Neighbors(person_oid_B, cache->post_has_creator_t, gdb::Ingoing));
  //postsB->Intersection(messages);

  //boost::scoped_ptr<gdb::Objects> repliedByA(graph->Neighbors(commentsA.get(), cache->reply_of_t, gdb::Outgoing));
  //boost::scoped_ptr<gdb::Objects> repliedByB(graph->Neighbors(commentsB.get(), cache->reply_of_t, gdb::Outgoing));

  
  boost::scoped_ptr<gdb::Objects> repliesToAPosts(graph->Neighbors(postsA.get(), cache->reply_of_t, gdb::Ingoing));
  boost::scoped_ptr<gdb::Objects> repliesToAComments(graph->Neighbors(commentsA.get(), cache->reply_of_t, gdb::Ingoing));

  boost::scoped_ptr<gdb::Objects> repliesToBPosts(graph->Neighbors(postsB.get(), cache->reply_of_t, gdb::Ingoing));
  boost::scoped_ptr<gdb::Objects> repliesToBComments(graph->Neighbors(commentsB.get(), cache->reply_of_t, gdb::Ingoing));

  boost::scoped_ptr<gdb::Objects> ArepliedToBComments(gdb::Objects::CombineIntersection(commentsA.get(), repliesToBComments.get()));
  boost::scoped_ptr<gdb::Objects> ArepliedToBPosts(gdb::Objects::CombineIntersection(commentsA.get(), repliesToBPosts.get()));
  boost::scoped_ptr<gdb::Objects> BrepliedToAComments(gdb::Objects::CombineIntersection(commentsB.get(), repliesToAComments.get()));
  boost::scoped_ptr<gdb::Objects> BrepliedToAPosts(gdb::Objects::CombineIntersection(commentsB.get(), repliesToAPosts.get()));

  ArepliedToBComments->Intersection(messages);
  ArepliedToBPosts->Intersection(messages);
  BrepliedToAComments->Intersection(messages);
  BrepliedToAPosts->Intersection(messages);

  return (BrepliedToAComments->Count()+ArepliedToBComments->Count())*0.5f+
         (ArepliedToBPosts->Count() + BrepliedToAPosts->Count())*1.0f;

}

void back_track(gdb::Session* sess, 
                gdb::Graph* graph, 
                snb::TypeCache* cache,
                std::vector<gdb::Objects*> levelsFWD,
                std::vector<gdb::Objects*> levelsBWD,
                std::vector<gdb::oid_t>& current_path, 
                gdb::oid_t dest, 
                std::vector<Result>& intermediate_result,
                std::map<gdb::oid_t, float>& weights,
                gdb::Objects* whiteList
                ) {
  int total_levels_including_first_and_last = (levelsFWD.size() + levelsBWD.size() /*-1*/);
  int current_level = total_levels_including_first_and_last - current_path.size() - 1; // -1 to skip the last level
  if( current_level == 0 )
  {
    gdb::Value val;
    current_path.push_back(dest); // The first
    Result res;
    res.weight = 0.0;
    for( int i = current_path.size()-1; i >= 1; --i) {
      gdb::oid_t edge_oid = graph->FindEdge(cache->knows_t, current_path[i], current_path[i-1]);
      double sim = 0.0;
      std::map<gdb::oid_t, float>::iterator it = weights.find(edge_oid);
      if(it == weights.end()) {
        sim = ComputeWeight(sess,graph,cache,current_path[i], current_path[i-1],whiteList);
        weights.insert(std::make_pair(edge_oid, sim ));
      } else {
        sim = it->second;
      }
      res.weight+=sim;
      graph->GetAttribute(current_path[i], cache->person_id_t, val);
      res.path.push_back(val.GetLong());
    }
    graph->GetAttribute(current_path[0], cache->person_id_t, val);
    res.path.push_back(val.GetLong());
    intermediate_result.push_back(res);
    current_path.pop_back();
  }
  else if (current_level >= (int) levelsFWD.size())
  {
    // Backwards levels
    gdb::Objects* neighbors = graph->Neighbors(current_path.back(), cache->knows_t, gdb::Outgoing);
    neighbors->Intersection(levelsBWD[current_path.size()]);
    gdb::ObjectsIterator* iter_neighbors = neighbors->Iterator();
    while(iter_neighbors->HasNext()) {
      gdb::oid_t neighbor = iter_neighbors->Next();
      current_path.push_back(neighbor);
      back_track(sess,graph,cache,levelsFWD, levelsBWD,current_path,dest,intermediate_result, weights, whiteList);
      current_path.pop_back();
    }
    delete iter_neighbors;
    delete neighbors;
  }
  else {
    // Forward levels
    gdb::Objects* neighbors = graph->Neighbors(current_path.back(), cache->knows_t, gdb::Outgoing);
    neighbors->Intersection(levelsFWD[current_level]);
    gdb::ObjectsIterator* iter_neighbors = neighbors->Iterator();
    while(iter_neighbors->HasNext()) {
      gdb::oid_t neighbor = iter_neighbors->Next();
      current_path.push_back(neighbor);
      back_track(sess,graph,cache,levelsFWD, levelsBWD,current_path,dest,intermediate_result, weights, whiteList);
      current_path.pop_back();
    }
    delete iter_neighbors;
    delete neighbors;
  }
}

bool CalculateNextLevel(
                        gdb::Session* sess,
                        gdb::Graph *graph,
                        snb::TypeCache *cache,
                        gdb::Objects* &visited,
                        std::vector<gdb::Objects*> &levels,
                        sparksee::gdb::Objects * destinationNodes,
                        unsigned int &currentLevel,
                        sparksee::gdb::EdgesDirection edgeDirection)
{
  bool found = false;
  gdb::Objects* next_level = NULL;
  if(levels[currentLevel]->Count() / (double)graph->GetType(cache->person_t)->GetNumObjects() < 0.5) {
    next_level = graph->Neighbors(levels[currentLevel++], cache->knows_t, edgeDirection);
    next_level->Difference(visited);
    visited->Union(next_level);
  } else {
    next_level = sess->NewObjects();
    boost::scoped_ptr<gdb::Objects> knows(graph->Select(cache->knows_t));
    boost::scoped_ptr<gdb::ObjectsIterator> knows_iter(knows->Iterator());
    while(knows_iter->HasNext()) {
      gdb::oid_t next_knows = knows_iter->Next();
      gdb::EdgeData* edge = graph->GetEdgeData(next_knows);
      bool tailExists = levels[currentLevel]->Exists(edge->GetTail());
      bool headExists = levels[currentLevel]->Exists(edge->GetHead());
      if(tailExists && !headExists) {
        next_level->Add(edge->GetHead());
        visited->Add(edge->GetHead());
      } 

      if(headExists && !tailExists) {
        next_level->Add(edge->GetTail());
        visited->Add(edge->GetTail());
      }
    }
    currentLevel++;
  }
  sparksee::gdb::Objects *bridge = gdb::Objects::CombineIntersection(next_level, destinationNodes);
  if (bridge->Count()>0)
  {
    found = true;
    // The "bridge" nodes may be smaller
    levels.push_back(bridge);
    delete next_level;
  }
  else
  {
    levels.push_back(next_level);
    delete bridge;
  }
  return found;
}

unsigned long GetLevelDegree(gdb::Graph* graph, snb::TypeCache* cache,  gdb::Objects* level) {
  unsigned long accum = 0;
  sparksee::utils::ObjectsIteratorPtr iter_level(level->Iterator());
  while(iter_level->HasNext()) {
    gdb::oid_t person_oid = iter_level->Next();
    accum+=graph->Degree(person_oid, cache->knows_t, gdb::Outgoing );
  }
  return accum;
}


void CleanLevels(gdb::Graph* graph, snb::TypeCache* cache,  gdb::Objects* origin, std::vector<gdb::Objects*> &levels, bool do_first ) {
  if(do_first && levels.size() > 0) {
    sparksee::utils::ObjectsPtr neighbors(graph->Neighbors(origin, cache->knows_t, gdb::Outgoing ));
    levels[levels.size() -1]->Intersection(neighbors.get());
  }
  for(int i = (levels.size() - 1); i > 1; --i) {
    sparksee::utils::ObjectsPtr neighbors(graph->Neighbors(levels[i], cache->knows_t, gdb::Outgoing ));
    levels[i-1]->Intersection(neighbors.get());
  }
}

datatypes::Buffer Execute(gdb::Session *sess, long long person1, long long person2, long long startDate, long long endDate) {
#ifdef VERBOSE
  printf("BI QUERY25: %lli %lli\n", person1, person2);
#endif

  BEGIN_EXCEPTION

    gdb::Graph *graph = sess->GetGraph();
  snb::TypeCache *cache = snb::TypeCache::instance(graph);
  std::vector<Result> intermediate_result;
  gdb::Value val;
  BEGIN_TRANSACTION
    //gdb::attr_t weight_attr_t = graph->NewSessionAttribute( cache->knows_t, gdb::Double, gdb::Basic);

  val.SetLong(person1);
  gdb::oid_t person1_oid = graph->FindObject(cache->person_id_t, val);
  val.SetLong(person2);
  gdb::oid_t person2_oid = graph->FindObject(cache->person_id_t, val);

  // FWD
  gdb::Objects* visitedFWD = sess->NewObjects();
  gdb::Objects* first_level = sess->NewObjects();
  first_level->Add(person1_oid);
  std::vector<gdb::Objects*> levelsFWD;
  levelsFWD.push_back(first_level);
  unsigned int currentLevelFWD = 0;

  // BWD
  gdb::Objects* visitedBWD = sess->NewObjects();
  gdb::Objects* last_level = sess->NewObjects();
  last_level->Add(person2_oid);
  std::vector<gdb::Objects*> levelsBWD;
  levelsBWD.push_back(last_level);
  unsigned int currentLevelBWD = 0;

  timeval start_time,end_time;
  gettimeofday(&start_time,NULL);
  // Search both ways
  //clock_t start = clock();
  bool foundFWD = false;
  bool foundBWD = false;
  while ((!foundFWD && !foundBWD) && (levelsFWD[currentLevelFWD]->Count()>0) && (levelsBWD[currentLevelBWD]->Count()>0))
  {
    // Search one level forward
    unsigned long forwardDegree = GetLevelDegree(graph, cache,levelsFWD[currentLevelFWD]);
    unsigned long backwardDegree = GetLevelDegree(graph, cache,levelsBWD[currentLevelBWD]);
    if(forwardDegree < backwardDegree ) {
      foundFWD = CalculateNextLevel( sess, graph, cache,
                                     visitedFWD, levelsFWD, levelsBWD[currentLevelBWD],
                                     currentLevelFWD, gdb::Outgoing);
    } else {
      foundBWD = CalculateNextLevel( sess, graph, cache,
                                     visitedBWD, levelsBWD, levelsFWD[currentLevelFWD],
                                     currentLevelBWD, gdb::Ingoing);
    }
  }

  delete visitedFWD;
  delete visitedBWD;

  if (!foundFWD && !foundBWD)
  {
    COMMIT_TRANSACTION
      std::string str("{}");
    char *result_str = new char[str.length() + 1];
    std::strcpy(result_str, str.c_str());
    for( unsigned int i = 0; i < levelsFWD.size(); ++i) {
      delete levelsFWD[i];
    }
    for( unsigned int i = 0; i < levelsBWD.size(); ++i) {
      delete levelsBWD[i];
    }
    delete graph;
    return datatypes::Buffer(result_str, strlen(result_str));
  }

  if(foundFWD) {
    delete levelsBWD.back();
    levelsBWD.pop_back();
    currentLevelBWD--;
  } else {
    delete levelsFWD.back();
    levelsFWD.pop_back();
    currentLevelFWD--;
  }

  gettimeofday(&end_time,NULL);
  unsigned long executionTime = (end_time.tv_sec - start_time.tv_sec)*1000 + (end_time.tv_usec - start_time.tv_usec)/1000;
  printf("PATH EXECUTION_TIME: %lu ms\n",executionTime);

  gettimeofday(&start_time,NULL);

  std::vector<gdb::oid_t> path;
  path.push_back(person2_oid);

  gdb::Value start;
  start.SetTimestamp(startDate);
  gdb::Value end;
  end.SetTimestamp(endDate);
  boost::scoped_ptr<gdb::Objects> forums(graph->Select(cache->forum_creation_date_t, gdb::Between, start, end));
  boost::scoped_ptr<gdb::Objects> whiteList(graph->Neighbors(forums.get(), cache->container_of_t, gdb::Outgoing));
  boost::scoped_ptr<gdb::Objects> next_frontier(sess->NewObjects());
  next_frontier->Union(whiteList.get());
  while(next_frontier->Count() != 0) {
    printf("%llu\n", next_frontier.get()->Count());
    gdb::Objects* next_step = graph->Neighbors(next_frontier.get(), cache->reply_of_t, gdb::Ingoing);
    whiteList->Union(next_step);
    next_frontier.reset(next_step);
  }

  gettimeofday(&end_time,NULL);
  unsigned long executionTime2 = (end_time.tv_sec - start_time.tv_sec)*1000 + (end_time.tv_usec - start_time.tv_usec)/1000;
  printf("WHITHELIST TIME: %lu ms\n",executionTime2);
  
  gettimeofday(&start_time,NULL);
  std::map<gdb::oid_t, float> weights;
  back_track(sess,graph,cache, levelsFWD, levelsBWD, path, person1_oid, intermediate_result, weights, whiteList.get());

  gettimeofday(&end_time,NULL);
  unsigned long executionTime3 = (end_time.tv_sec - start_time.tv_sec)*1000 + (end_time.tv_usec - start_time.tv_usec)/1000;
  printf("BACKTRACK TIME: %lu ms\n",executionTime3);

  std::sort(intermediate_result.begin(), intermediate_result.end(),
            compare_result);

  ptree::ptree pt = Project(*sess,*graph, *cache, intermediate_result);
  for( int i = 0; i < static_cast<int>(levelsFWD.size()); ++i) {
    delete levelsFWD[i];
  }
  for( int i = 0; i < static_cast<int>(levelsBWD.size()); ++i) {
    delete levelsBWD[i];
  }
  COMMIT_TRANSACTION
    delete graph;
#ifdef VERBOSE
  printf("EXIT BI QUERY25: %lli %lli %li\n", person1, person2, intermediate_result.size());
#endif
  char* res = snb::utils::to_json(pt);

  return datatypes::Buffer(res, strlen(res));
  END_EXCEPTION
}
}
}

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
namespace query22 {

struct Value {
    Value(long long person1_id, long long person2_id, int score) : interaction_score(score), id1(person1_id), id2(person2_id) {}

    int interaction_score;
    long long id1;
    long long id2;
};

struct Result {

    Result(const std::string& str, long long id1, long long id2,int score)
      : key(str), value(id1, id2, score) {}

    static Result create(std::map<std::string,Value>::value_type& val) {
      return Result(val.first, val.second.id1, val.second.id2, val.second.interaction_score);
    }

    bool operator<(const Result &rhs) const {
      if ( value.interaction_score != rhs.value.interaction_score ) 
        return sparksee::utils::descending(value.interaction_score, rhs.value.interaction_score);
      if( value.id1 != rhs.value.id1 ) {
        return sparksee::utils::ascending(value.id1, rhs.value.id1);
      }
      return sparksee::utils::ascending(value.id2, rhs.value.id2);
    }

    std::string key;
    Value value;
};


bool compare_result(const Result &lhs, const Result &rhs) {
  return lhs < rhs;
}

struct compare_priority {
  bool operator()(const Result &lhs, const Result &rhs) {
    return compare_result(lhs, rhs);
  }
};


static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Result &result) {
    gdb::Value val;
    ptree::ptree pt;

    pt.put<long long>("Person.id1", result.value.id1);
    pt.put<long long>("Person.id2", result.value.id2);
    pt.put<std::string>("City.name", result.key);
    pt.put<int>("score", result.value.interaction_score);

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

struct PersonPair {
  gdb::oid_t person_oid;
  long long  person_id;

  PersonPair( ) {
  }

  PersonPair( gdb::oid_t person_oid, long long person_id ) {
    this->person_oid = person_oid;
    this->person_id = person_id;
  }

  bool operator<(const PersonPair& rhs) {
    return person_id < rhs.person_id;
  }
};

datatypes::Buffer Execute(gdb::Session *sess, const char* country1, const char* country2, int limit) {
#ifdef VERBOSE
    printf("Bi QUERY22: %s %s\n", country1, country2);
#endif

    BEGIN_EXCEPTION

    boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
    snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

    BEGIN_TRANSACTION;

    gdb::Value value;
    long long country1_oid = graph->FindObject(cache->place_name_t, value.SetString(sparksee::utils::to_wstring(country1)));
    long long country2_oid = graph->FindObject(cache->place_name_t, value.SetString(sparksee::utils::to_wstring(country2)));

    std::map<gdb::oid_t, std::vector<PersonPair>*> persons_per_city1;
    boost::scoped_ptr<gdb::Objects> country1_persons(snb::utils::people_from_country(country1_oid, *graph.get()));
    boost::scoped_ptr<gdb::ObjectsIterator> country1_persons_iter(country1_persons->Iterator());
    while(country1_persons_iter->HasNext()) {
      gdb::oid_t next_person = country1_persons_iter->Next();
      boost::scoped_ptr<gdb::Objects> city(graph->Neighbors(next_person, cache->is_located_in_t, gdb::Outgoing));
      gdb::oid_t city_oid = city->Any();
      if(persons_per_city1.find(city_oid) == persons_per_city1.end()) {
        persons_per_city1.insert(std::make_pair(city_oid,new std::vector<PersonPair>()));
      }
      graph->GetAttribute(next_person,cache->person_id_t, value);
      persons_per_city1[city_oid]->push_back(PersonPair(next_person, value.GetLong()));
    }

    for( std::map<gdb::oid_t, std::vector<PersonPair>*>::iterator it = persons_per_city1.begin(); 
         it != persons_per_city1.end();
         ++it) {
      std::sort(it->second->begin(), it->second->end());
    }


    std::vector<PersonPair> persons_country2;
    boost::scoped_ptr<gdb::Objects> country2_persons(snb::utils::people_from_country(country2_oid, *graph.get()));
    boost::scoped_ptr<gdb::ObjectsIterator> country2_persons_iter(country2_persons->Iterator());
    while(country2_persons_iter->HasNext()) {
      gdb::oid_t next_person = country2_persons_iter->Next();
      graph->GetAttribute(next_person,cache->person_id_t, value);
      persons_country2.push_back(PersonPair(next_person, value.GetLong()));
    }
    std::sort(persons_country2.begin(), persons_country2.end());

    std::map<std::string, Value> relation_map;
    for( std::map<gdb::oid_t, std::vector<PersonPair>*>::iterator iter_city1 = persons_per_city1.begin(); 
         iter_city1 != persons_per_city1.end();
         ++iter_city1) {

      graph->GetAttribute(iter_city1->first, cache->place_name_t, value);
      std::string city_name = sparksee::utils::to_string(value.GetString());

      PersonPair personPair1(0,0);
      PersonPair personPair2(0,0);
      int best_score = -1;

      for(std::vector<PersonPair>::iterator iter_persons1 = iter_city1->second->begin();
          iter_persons1 != iter_city1->second->end() && best_score < 31;
          iter_persons1++
         ) {

        gdb::oid_t person1_oid = iter_persons1->person_oid;

        boost::scoped_ptr<gdb::Objects> likes(graph->Neighbors(person1_oid, cache->likes_t, gdb::Outgoing));
        boost::scoped_ptr<gdb::Objects> messages(graph->Neighbors(person1_oid, cache->post_has_creator_t, gdb::Ingoing));
        messages->Union(boost::scoped_ptr<gdb::Objects>(graph->Neighbors(person1_oid, cache->comment_has_creator_t, gdb::Ingoing)).get());
        boost::scoped_ptr<gdb::Objects> replied(graph->Neighbors(messages.get(), cache->reply_of_t, gdb::Outgoing));

        for( std::vector<PersonPair>::iterator iter_persons2 = persons_country2.begin(); 
             iter_persons2 != persons_country2.end() && best_score < 31;
             ++iter_persons2) {

          gdb::oid_t person2_oid = iter_persons2->person_oid;
          int score = 0;

          boost::scoped_ptr<gdb::Objects> likes2(graph->Neighbors(person2_oid, cache->likes_t, gdb::Outgoing));
          boost::scoped_ptr<gdb::Objects> messages2(graph->Neighbors(person2_oid, cache->post_has_creator_t, gdb::Ingoing));
          messages2->Union(boost::scoped_ptr<gdb::Objects>(graph->Neighbors(person2_oid, cache->comment_has_creator_t, gdb::Ingoing)).get());
          boost::scoped_ptr<gdb::Objects> replied2(graph->Neighbors(messages2.get(), cache->reply_of_t, gdb::Outgoing));
          score += (graph->FindEdge(cache->knows_t, person1_oid, person2_oid) != gdb::Objects::InvalidOID) ? 15 : 0;
          score += boost::scoped_ptr<gdb::Objects>(gdb::Objects::CombineIntersection(replied.get(), messages2.get()))->Count() > 0 ? 4 : 0;
          score += boost::scoped_ptr<gdb::Objects>(gdb::Objects::CombineIntersection(replied2.get(), messages.get()))->Count() > 0 ? 1 : 0;
          score += boost::scoped_ptr<gdb::Objects>(gdb::Objects::CombineIntersection(likes.get(), messages2.get()))->Count() > 0 ? 10 : 0;
          score += boost::scoped_ptr<gdb::Objects>(gdb::Objects::CombineIntersection(likes2.get(), messages.get()))->Count() > 0 ? 1 : 0;

          if(score > best_score) {
            best_score = score;
            personPair1 = *iter_persons1;
            personPair2 = *iter_persons2;
          }
        }
      }
      relation_map.insert(std::make_pair(city_name, Value(personPair1.person_id, personPair2.person_id, best_score)));
    }

    for( std::map<gdb::oid_t, std::vector<PersonPair>*>::iterator it = persons_per_city1.begin(); 
         it != persons_per_city1.end();
         ++it) {
      delete it->second;
    }

    /*std::map<std::string, Value> relation_map;
    boost::scoped_ptr<gdb::ObjectsIterator> person1_iterator(country1_persons->Iterator());
    while (person1_iterator->HasNext()) {
        long long person1_oid = person1_iterator->Next();
        graph->GetAttribute(person1_oid, cache->person_id_t, value);
        long long person1_id = value.GetLong();
        boost::scoped_ptr<gdb::Objects> city(graph->Neighbors(person1_oid, cache->is_located_in_t, gdb::Outgoing));
        gdb::oid_t city_id = city->Any();
        graph->GetAttribute(city_id, cache->place_name_t, value);
        std::string city_name = sparksee::utils::to_string(value.GetString());

        boost::scoped_ptr<gdb::Objects> likes(graph->Neighbors(person1_oid, cache->likes_t, gdb::Outgoing));
        boost::scoped_ptr<gdb::Objects> messages(graph->Neighbors(person1_oid, cache->post_has_creator_t, gdb::Ingoing));
        messages->Union(boost::scoped_ptr<gdb::Objects>(graph->Neighbors(person1_oid, cache->comment_has_creator_t, gdb::Ingoing)).get());
        boost::scoped_ptr<gdb::Objects> replied(graph->Neighbors(messages.get(), cache->reply_of_t, gdb::Outgoing));

        boost::scoped_ptr<gdb::ObjectsIterator> person2_iterator(country2_persons->Iterator());
        while (person2_iterator->HasNext()) {
            long long person2_oid = person2_iterator->Next();
            graph->GetAttribute(person2_oid, cache->person_id_t, value);
            long long person2_id = value.GetLong();
            int score = 0;

            boost::scoped_ptr<gdb::Objects> likes2(graph->Neighbors(person2_oid, cache->likes_t, gdb::Outgoing));
            boost::scoped_ptr<gdb::Objects> messages2(graph->Neighbors(person2_oid, cache->post_has_creator_t, gdb::Ingoing));
            messages2->Union(boost::scoped_ptr<gdb::Objects>(graph->Neighbors(person2_oid, cache->comment_has_creator_t, gdb::Ingoing)).get());
            boost::scoped_ptr<gdb::Objects> replied2(graph->Neighbors(messages2.get(), cache->reply_of_t, gdb::Outgoing));
            score += (graph->FindEdge(cache->knows_t, person1_oid, person2_oid) != gdb::Objects::InvalidOID) ? 15 : 0;
            score += boost::scoped_ptr<gdb::Objects>(gdb::Objects::CombineIntersection(replied.get(), messages2.get()))->Count() > 0 ? 4 : 0;
            score += boost::scoped_ptr<gdb::Objects>(gdb::Objects::CombineIntersection(replied2.get(), messages.get()))->Count() > 0 ? 1 : 0;
            score += boost::scoped_ptr<gdb::Objects>(gdb::Objects::CombineIntersection(likes.get(), messages2.get()))->Count() > 0 ? 10 : 0;
            score += boost::scoped_ptr<gdb::Objects>(gdb::Objects::CombineIntersection(likes2.get(), messages.get()))->Count() > 0 ? 1 : 0;

            std::map<std::string,Value>::iterator it = relation_map.find(city_name);
            if(it == relation_map.end()) {
              relation_map.insert(std::make_pair(city_name, Value(person1_id, person2_id, score)));
            } else if(score > it->second.interaction_score){
                it->second.id1 = person1_id ;
                it->second.id2 = person2_id;
                it->second.interaction_score = score;
            } else if( score == it->second.interaction_score) {
              if(it->second.id1 > person1_id || (it->second.id1 == person1_id && it->second.id2 > person2_id )) {
                it->second.id1 = person1_id ;
                it->second.id2 = person2_id;
                it->second.interaction_score = score;
              }
            }

        }
    }
    */

    std::vector<Result> intermediate_result;
    std::transform(relation_map.begin(), relation_map.end(), std::back_inserter(intermediate_result), &Result::create);
    std::sort(intermediate_result.begin(), intermediate_result.end(), compare_result);
    ptree::ptree pt = Project(*graph, *cache, intermediate_result);
    COMMIT_TRANSACTION

#ifdef VERBOSE
    printf("EXIT BI QUERY22: %s %s %lu\n", country1, country2, intermediate_result.size());
#endif
    char* ret = snb::utils::to_json(pt);
    return datatypes::Buffer(ret, strlen(ret));

    END_EXCEPTION
}
}
}

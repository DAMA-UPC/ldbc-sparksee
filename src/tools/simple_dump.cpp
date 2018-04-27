

#include "gdb/Sparksee.h"
#include "gdb/Database.h"
#include "gdb/Session.h"
#include "gdb/Graph.h"
#include "gdb/Objects.h"
#include "gdb/ObjectsIterator.h"
#include "gdb/Exception.h"
#include "script/ScriptParser.h"
#include <iostream>

#include "queries/Utils.h"
#include "queries/TypeCache.h"
#include "utils/Utils.h"

#include "threads/Threads.h"

#include <map>
#include <fstream>

using namespace sparksee::gdb;

SparkseeConfig cfg;
Sparksee *gdb;
Database *db;
int main(int argc, char *argv[]) {

	std::string str(argv[1]);

	gdb = new Sparksee(cfg);
	db = gdb->Open(std::wstring(str.begin(), str.end()), false);
  Session* session = db->NewSession();
  Graph* graph = session->GetGraph();
  sparksee::snb::TypeCache* cache = sparksee::snb::TypeCache::instance(graph);

  std::map<oid_t, int> person_dictionary;
  int next_index = 0;
  std::ofstream persons_output("persons.csv");

  Objects* persons = graph->Select(cache->person_t);
  ObjectsIterator* iter_persons = persons->Iterator();
  while(iter_persons->HasNext()) {
    oid_t next_person = iter_persons->Next();


    Objects* city_objects = graph->Neighbors(next_person, cache->is_located_in_t, Outgoing );
    oid_t city = city_objects->Any();
    delete city_objects;
    Objects* country_objects = graph->Neighbors(city,cache->is_part_of_t, Outgoing);
    oid_t country = country_objects->Any();
    delete country_objects;
    Objects* university_objects = graph->Neighbors(next_person, cache->study_at_t, Outgoing);
    if(university_objects->Count() > 0) {
      oid_t university = university_objects->Any();

      person_dictionary[next_person] = next_index;
      persons_output << (next_index) << "|" << sparksee::utils::to_string(graph->GetAttribute(country,cache->place_name_t)->GetString()) << "|" << sparksee::utils::to_string(graph->GetAttribute(university,cache->organization_name_t)->GetString()) << "|" << sparksee::utils::to_string(graph->GetAttribute(next_person,cache->person_gender_t)->GetString()) << std::endl;
      next_index+=1;
    }
    delete university_objects;
  }

  persons_output.close();
  delete iter_persons;

  std::ofstream graph_output("graph.csv");
  iter_persons = persons->Iterator();
  while(iter_persons->HasNext()) {
    oid_t next_person = iter_persons->Next();
    Objects* neighbors = graph->Neighbors(next_person, cache->knows_t, Outgoing);
    ObjectsIterator* iter_neighbors = neighbors->Iterator();
    while(iter_neighbors->HasNext()) {
      oid_t neighbor = iter_neighbors->Next();
      if(person_dictionary.find(next_person) != person_dictionary.end() && 
          person_dictionary.find(neighbor) != person_dictionary.end()
          ) {
        int first = person_dictionary[next_person];
        int second = person_dictionary[neighbor];
        if(first < second ) {
          graph_output << first << "|" << second << std::endl;
        }
      }
    }
    delete iter_neighbors;
    delete neighbors;
  }
  delete iter_persons;

  graph_output.close();

  delete persons;
  delete graph;
  delete session;
  delete db;
  delete gdb;
}

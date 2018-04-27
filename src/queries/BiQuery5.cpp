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
namespace query5 {

typedef struct forumSorter_ {

  forumSorter_(long long foid, long long fid, int fcount)
      : oid(foid), id(fid), count(fcount) {}

    long long oid;
    long long id;
    int count;
    bool operator<(const forumSorter_ &b) const {
      if (count != b.count)
        return sparksee::utils::descending<int>(count,b.count);
      return sparksee::utils::ascending<long long>(id,b.id);
    }
} ForumSorter;

typedef struct key_ {

  key_(long long pid, int pcount, std::string &pfirst_name,
       std::string &plast_name, long long pcreation_date)
      : person_id(pid), count(pcount), first_name(pfirst_name),
        last_name(plast_name), creation_date(pcreation_date) {}

    long long person_id;
    int count;
    std::string first_name;
    std::string last_name;
    long long creation_date;
    bool operator<(const key_ &b) const {
      if (count != b.count)
        return count > b.count;
      if (person_id != b.person_id)
        return person_id < b.person_id;
      if (first_name != b.first_name)
        return first_name < b.first_name;
      if (last_name != b.last_name)
        return last_name < b.last_name;
      if (creation_date != b.creation_date)
        return creation_date < b.creation_date;
      return false;
    }
} Key;

typedef struct {
} Value;

struct Result {

    Result(long long pid, int pcount, std::string &pfirst_name,
         std::string &plast_name, long long pcreation_date)
      : key(pid, pcount, pfirst_name, plast_name, pcreation_date) {}

    Key key;
    Value value;
};

bool compare_result(const Result &lhs, const Result &rhs) {
  if (lhs.key.count != rhs.key.count) {
      return sparksee::utils::descending(lhs.key.count, rhs.key.count);
  }
  return sparksee::utils::ascending(lhs.key.person_id, rhs.key.person_id);
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Result &result) {
    gdb::Value val;
    ptree::ptree pt;

    pt.put<long long>("Person.id", result.key.person_id);
    pt.put<std::string>("Person.firstName", result.key.first_name);
    pt.put<std::string>("Person.lastName", result.key.last_name);
    pt.put<long long>("Person.creationDate", result.key.creation_date);
    pt.put<int>("count", result.key.count);

    return pt;
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache,
                            const std::vector<Result> &results, unsigned int limit) {
    ptree::ptree pt;
    unsigned int counter = 0;
    for (std::vector<Result>::const_iterator it = results.begin();
        it != results.end() && counter < limit; ++it, counter++) {
      pt.push_back(std::make_pair("", Project(graph, cache, *it)));
    }
    return pt;
}

datatypes::Buffer Execute(gdb::Session *sess, const char* country, unsigned int limit) {
#ifdef VERBOSE
    printf("Bi QUERY5: %s %d\n", country, limit);
#endif

    BEGIN_EXCEPTION

    boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
    snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

    BEGIN_TRANSACTION;

    gdb::Value value;
    //long long country_oid = graph->FindObject(cache->place_name_t, value.SetString(sparksee::utils::to_wstring(country)));
    long long country_oid = graph->FindObject(cache->place_name_t, value.SetString(sparksee::utils::to_wstring("China")));

    timeval start,end;
    gettimeofday(&start,NULL);

    boost::scoped_ptr<gdb::Objects> cities(
        graph->Neighbors(country_oid, cache->is_part_of_t, gdb::Ingoing));

    boost::scoped_ptr<gdb::Objects> country_persons(
        graph->Neighbors(cities.get(), cache->is_located_in_t, gdb::Ingoing));

    std::map<gdb::oid_t, long> forums_count;

    boost::scoped_ptr<gdb::ObjectsIterator> person_iterator(country_persons->Iterator());
    while (person_iterator->HasNext()) {
        long long person_oid = person_iterator->Next();
        boost::scoped_ptr<gdb::Objects> person_forums(
            graph->Neighbors(person_oid, cache->has_member_t, gdb::Ingoing));
        boost::scoped_ptr<gdb::ObjectsIterator> iter_forums(person_forums->Iterator());
        while(iter_forums->HasNext()) {
          long long forum_oid = iter_forums->Next();
          forums_count[forum_oid]++;
        }
    }

    /*boost::scoped_ptr<gdb::Objects> has_member(graph->Select(cache->has_member_t));
    boost::scoped_ptr<gdb::ObjectsIterator> iter_members(has_member->Iterator());
    while(iter_members->HasNext()) {
      gdb::oid_t next_membership = iter_members->Next();
      gdb::EdgeData* edge = graph->GetEdgeData(next_membership);
      if(country_persons->Exists(edge->GetHead())) {
        forums_count[edge->GetTail()]++;
      }
    }*/


    std::vector<ForumSorter> forums;
    for(std::map<gdb::oid_t, long>::iterator it = forums_count.begin(); it != forums_count.end(); ++it) {
          graph->GetAttribute(it->first, cache->forum_id_t, value);
          forums.push_back(ForumSorter(it->first,value.GetLong(), it->second));
    }
    

    std::sort(forums.begin(), forums.end());
    if (forums.size() > 100) {
        forums.erase(forums.begin() + 100, forums.end());
    }

    gettimeofday(&end,NULL);
    unsigned long executionTime = (end.tv_sec - start.tv_sec)*1000 + (end.tv_usec - start.tv_usec)/1000;
    printf("JOIN EXECUTION_TIME: %lu ms\n",executionTime);


    boost::scoped_ptr<gdb::Objects> country_forums_members(sess->NewObjects());
    boost::scoped_ptr<gdb::Objects> country_forums_posts(sess->NewObjects());
    for (std::vector<ForumSorter>::iterator it = forums.begin(); it != forums.end(); ++it) {
        ForumSorter forum = *it;
        graph->GetAttribute(forum.oid, cache->forum_id_t, value);
        boost::scoped_ptr<gdb::Objects> persons(graph->Neighbors(forum.oid, cache->has_member_t, gdb::Outgoing));
        boost::scoped_ptr<gdb::Objects> posts(graph->Neighbors(forum.oid, cache->container_of_t, gdb::Outgoing));
        boost::scoped_ptr<gdb::Objects> moderator(graph->Neighbors(forum.oid, cache->has_moderator_t, gdb::Outgoing));
        country_forums_members->Union(persons.get());
        country_forums_members->Add(moderator->Any());
        country_forums_posts->Union(posts.get());
    }

    gdb::Value id_value;
    gdb::Value first_name_value;
    gdb::Value last_name_value;
    gdb::Value creation_value;
    std::vector<Result> intermediate_result;

    boost::scoped_ptr<gdb::ObjectsIterator> iter_persons(country_forums_members->Iterator());
    while (iter_persons->HasNext()) {
      long long person_oid = iter_persons->Next();
      boost::scoped_ptr<gdb::Objects> person_posts(graph->Neighbors(person_oid, cache->post_has_creator_t, gdb::Ingoing));
      person_posts->Intersection(country_forums_posts.get());
      graph->GetAttribute(person_oid, cache->person_id_t, id_value);
      graph->GetAttribute(person_oid, cache->person_first_name_t, first_name_value);
      graph->GetAttribute(person_oid, cache->person_last_name_t, last_name_value);
      graph->GetAttribute(person_oid, cache->person_creation_date_t, creation_value);
      std::string first_name = sparksee::utils::to_string(first_name_value.GetString());
      std::string last_name = sparksee::utils::to_string(last_name_value.GetString());
      Result res(id_value.GetLong(), person_posts->Count(), first_name,
          last_name, creation_value.GetTimestamp());
      intermediate_result.push_back(res);
    }
    std::sort(intermediate_result.begin(), intermediate_result.end(),
              compare_result);
    ptree::ptree pt = Project(*graph, *cache, intermediate_result, limit);
    COMMIT_TRANSACTION

#ifdef VERBOSE
    printf("EXIT BI QUERY5: %s %lu\n", country, intermediate_result.size());
#endif
    char* ret = snb::utils::to_json(pt);
    return datatypes::Buffer(ret, strlen(ret));

    END_EXCEPTION
}
}
}

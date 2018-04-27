#include "Utils.h"
#include <gdb/Objects.h>
#include <gdb/Graph.h>
#include <gdb/Objects.h>
#include <gdb/Session.h>
#include <gdb/ObjectsIterator.h>
#include <string>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boostPatch/property_tree/json_parser.hpp>
#include <utf8.h>
#include <sstream>

namespace sparksee {
namespace snb {
namespace utils {

gdb::Objects* get_tags(long long classtag_oid, gdb::Graph &graph) {
    snb::TypeCache *cache = snb::TypeCache::instance(&graph);
    boost::scoped_ptr<gdb::Objects> all_classes(graph.Neighbors(classtag_oid, cache->is_subclass_of_t, gdb::Ingoing));
    int size = all_classes->Count() - 1;
    do {
        size = all_classes->Count();
        boost::scoped_ptr<gdb::Objects> classes(graph.Neighbors(all_classes.get(), cache->is_subclass_of_t, gdb::Ingoing));
        all_classes->Union(classes.get());
    } while (size != all_classes->Count());
    return graph.Neighbors(all_classes.get(), cache->has_type_t, gdb::Ingoing);
}

gdb::Objects* people_from_country(long long country_oid, gdb::Graph &graph) {
    snb::TypeCache *cache = snb::TypeCache::instance(&graph);
    boost::scoped_ptr<gdb::Objects> country_cities(graph.Neighbors(country_oid, cache->is_part_of_t, gdb::Ingoing));
    gdb::Objects* country_persons = graph.Neighbors(country_cities.get(), cache->is_located_in_t, gdb::Ingoing);
    boost::scoped_ptr<gdb::Objects> persons(graph.Select(cache->person_t));
    country_persons->Intersection(persons.get());
    return country_persons;
}


char* to_json( boost::property_tree::ptree& ptree ) {
    std::stringstream ss;
    boost::property_tree::json_parser::write_json(ss, ptree, false );
    char *result_str = new char[ss.str().length() + 1];
    std::strcpy(result_str, ss.str().c_str());
    return result_str;
}

char* empty_json() {
  char* result_str = new char[3];
  result_str[0] = '{';
  result_str[1] = '}';
  result_str[2] = '\0';
  return result_str;
}


double similarity(gdb::Session& sess, 
    gdb::Graph& graph, 
    snb::TypeCache& cache,
    gdb::oid_t person1_oid, 
    gdb::oid_t person2_oid) {
  double weight = 0.0;
  gdb::Objects* posts1 = graph.Neighbors(person1_oid, cache.post_has_creator_t, gdb::Ingoing);
  gdb::Objects* posts2 = graph.Neighbors(person2_oid, cache.post_has_creator_t, gdb::Ingoing);
  gdb::Objects* comments1 = graph.Neighbors(person1_oid, cache.comment_has_creator_t, gdb::Ingoing);
  gdb::Objects* comments2 = graph.Neighbors(person2_oid, cache.comment_has_creator_t, gdb::Ingoing);
  gdb::ObjectsIterator* iter_comments = comments1->Iterator();
  while(iter_comments->HasNext()) {
    gdb::oid_t comment_oid = iter_comments->Next();
    gdb::Objects* replies = graph.Neighbors(comment_oid, cache.reply_of_t, gdb::Outgoing); 
if(replies->Count() == 0) {
gdb::Value val;
graph.GetAttribute(comment_oid,cache.comment_id_t,val);
std::cout << "Comment failing without replee " << val.GetLong() << std::endl;
}
    gdb::oid_t reply_oid = replies->Any();
    if( comments2->Exists(reply_oid) ) weight += 0.5;
    if( posts2->Exists(reply_oid) ) weight += 1.0;
    delete replies;
  }
  delete iter_comments;
  iter_comments = comments2->Iterator();
  while(iter_comments->HasNext()) {
    gdb::oid_t comment_oid = iter_comments->Next();
    gdb::Objects* replies = graph.Neighbors(comment_oid, cache.reply_of_t, gdb::Outgoing); 
if(replies->Count() == 0) {
gdb::Value val;
graph.GetAttribute(comment_oid,cache.comment_id_t,val);
std::cout << "Comment failing without replee " << val.GetLong() << std::endl;
}
    gdb::oid_t reply_oid = replies->Any();
    if( comments1->Exists(reply_oid) ) weight += 0.5;
    if( posts1->Exists(reply_oid) ) weight += 1.0;
    delete replies;
  }
  delete iter_comments;
  delete comments1;
  delete posts1;
  delete comments2;
  delete posts2;
  return weight;
}

void write_str( char** buffer, int &buffer_size, int & position,  const std::string& str ) {
  int len = str.length() + 1;
  int str_size = len + sizeof(int);
  if((int)(position + str_size) >= buffer_size ) {
    int new_size = buffer_size > str_size ? buffer_size*2 : buffer_size*2 + str_size ;
    char* new_buffer = new char[new_size];
    memcpy(new_buffer, *buffer, buffer_size);
    buffer_size=new_size;
    free(*buffer);
    *buffer = new_buffer;
  }
  write<int>(buffer, buffer_size, position, len );
  memcpy(static_cast<void*>(&(*buffer)[position]), (void*)str.c_str(), sizeof(char)*len);
  position+=len;
}

long long country_date( long long country_id, long long date ) {
    return (country_id << 56) + ((date >> 8) & 0x00ffffffffffffffff);
}

unsigned long date(unsigned int year, unsigned int month, unsigned int day) {
  boost::posix_time::ptime time_t_epoch(boost::gregorian::date(1970, 1, 1));
  /*boost::posix_time::ptime pdate = 
      boost::posix_time::microsec_clock::universal_time();*/
  boost::posix_time::ptime pdate(boost::gregorian::date(year,month,day)); 
  boost::posix_time::time_duration diff = pdate - time_t_epoch;
  return static_cast<unsigned long>(diff.total_milliseconds());
}

}
}
}

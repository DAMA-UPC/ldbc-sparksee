

#include "Utils.h"
#include "GroupBy.h"
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
namespace utils {

gdb::Objects *k_hop(gdb::Session &sess, const gdb::Graph &graph,
                    const gdb::oid_t oid, const gdb::type_t edge,
                    const gdb::EdgesDirection dir, const int num_hops,
                    const bool include_src) {
  if (num_hops <= 0)
    return NULL;
  gdb::Objects *result = sess.NewObjects();
  gdb::Objects *next_hop = sess.NewObjects();
  next_hop->Add(oid);
  for (int current_hop = 0; current_hop < num_hops; ++current_hop) {
      gdb::Objects *tmp =
          const_cast<gdb::Graph &>(graph).Neighbors(next_hop, edge, dir);
      delete next_hop;
      next_hop = tmp;
      result->Union(next_hop);
  }
  delete next_hop;
  if (include_src) {
    result->Add(oid);
  } else {
    result->Remove(oid);
  }
  return result;
}


gdb::Objects *select(gdb::Session &sess, const gdb::Graph &graph,
                     const gdb::type_t  edge_t, const gdb::EdgesDirection dir,
                     const gdb::attr_t attr_t, gdb::Condition condition, gdb::Value& val ) {
    gdb::Graph& nc_graph = const_cast<gdb::Graph &>(graph);
    gdb::Objects* selected = nc_graph.Select( attr_t, condition, val );
    gdb::Objects* ret = nc_graph.Neighbors(selected, edge_t, dir == gdb::Outgoing ? gdb::Ingoing : gdb::Outgoing );
    delete selected;
    return ret;
}

gdb::Objects *select(gdb::Session &sess, const gdb::Graph &graph,
                     const gdb::type_t  edge_t, const gdb::EdgesDirection dir,
                     const gdb::attr_t attr_t, gdb::Condition condition, gdb::Value& val, gdb::Objects* restriction ) {
    gdb::Graph& nc_graph = const_cast<gdb::Graph &>(graph);
    gdb::Value ret_val;
    gdb::Objects* ret = sess.NewObjects();
    gdb::ObjectsIterator* iter_restricted = restriction->Iterator();
    while(iter_restricted->HasNext()) {
        gdb::oid_t next = iter_restricted->Next();
        gdb::Objects* neighbors = nc_graph.Neighbors(next, edge_t, dir);
        gdb::oid_t other = neighbors->Any();
        nc_graph.GetAttribute(other, attr_t, ret_val);
        if( ret_val.Equals(val) ) ret->Add(next);
        delete neighbors;
    }
    delete iter_restricted;
    return ret;
}



GroupBy* group_by( gdb::Session& sess, const gdb::Graph& graph, gdb::Objects* edges, GroupBy::Mode mode ) {
    GroupBy* groupBy = new GroupBy(sess, graph);
    groupBy->execute(edges,mode);
    return groupBy;
}

std::wstring to_wstring( const std::string& s) {
   std::wstring utf16line;
   utf8::utf8to16(s.begin(), s.end(), back_inserter(utf16line));
   return utf16line;
}

std::wstring to_wstring( const char *s) {
  std::string an_string(s); 
  return to_wstring(an_string);
}

std::string to_string(const wchar_t *ws) {
    return to_string(std::wstring(ws));
}

std::string to_string(const std::wstring &ws) {
  std::string ret = "";
  for (std::basic_string<wchar_t>::const_iterator it = ws.begin(); it != ws.end(); ++it) {
    unsigned long long unicode = static_cast<unsigned long long>(*it);
    char utf8str[5] = {0, 0, 0, 0, 0};
    if (unicode < 0x7F) {                                                                           
      utf8str[0] = (unicode & 0x7F);     
    } else if (unicode < 0x7FF) {                                                                   
      for (int i = 1; i > 0; --i, unicode = unicode >>6) {                                        
        utf8str[i] = (unicode & 0x3F) | 0x80;                                                   
      }                                                                                           
      utf8str[0] = (unicode & 0x1F) | 0xC0;                                                       
    } else if (unicode < 0xFFFF ) {                                                                 
      for (int i = 2; i > 0; --i, unicode = unicode >>6) {                                        
        utf8str[i] = (unicode & 0x3F) | 0x80;                                                   
      }                                                                                           
      utf8str[0] = (unicode & 0x0F) | 0xE0;                                                       
    } else if (unicode < 0x1FFFFF) {                                                                
       for (int i = 3; i > 0; --i, unicode = unicode >>6) {                                        
         utf8str[i] = (unicode & 0x3F) | 0x80;                                                   
       }                                                                                           
       utf8str[0] = (unicode & 0x07) | 0xF0;                                                       
    }                                                                                               
    ret += utf8str;
  }
  return ret;
}

std::string to_string(const long long i) {
  char c[64];
  std::sprintf(c, "%lld", i);
  return std::string(c);
}

std::string to_string(const long i) {
  char c[64];
  std::sprintf(c, "%ld", i);
  return std::string(c);
}

std::string to_string(const int i) {
  char c[64];
  std::sprintf(c, "%d", i);
  return std::string(c);
}

std::string format_datetime(long long timestamp) {
  boost::posix_time::ptime time(boost::gregorian::date(1970, 1, 1),
                                boost::posix_time::milliseconds(timestamp));
  std::string str_time = boost::posix_time::to_iso_extended_string(time);
  str_time.replace(str_time.begin(), str_time.end(), ',', '.');
  str_time.erase(str_time.begin() + str_time.find('.') + 3);
  str_time.append("+0000");
  return str_time;
}

std::string format_date(long long timestamp) {
  boost::posix_time::ptime time(boost::gregorian::date(1970, 1, 1),
                                boost::posix_time::milliseconds(timestamp));
  boost::gregorian::date date = time.date();
  std::stringstream ss;
  ss << date.year() << "-" << date.month().as_number() << "-" << date.day();
  return ss.str();
}

unsigned int month( long long timestamp ) {
  boost::posix_time::ptime time(boost::gregorian::date(1970, 1, 1),
                                boost::posix_time::milliseconds(timestamp));
  boost::gregorian::date date = time.date();
  return date.month();
}

unsigned int year( long long timestamp ) {
  boost::posix_time::ptime time(boost::gregorian::date(1970, 1, 1),
                                boost::posix_time::milliseconds(timestamp));
  boost::gregorian::date date = time.date();
  return date.year();
}

unsigned int day( long long timestamp ) {
  boost::posix_time::ptime time(boost::gregorian::date(1970, 1, 1),
                                boost::posix_time::milliseconds(timestamp));
  boost::gregorian::date date = time.date();
  return date.day();
}

unsigned int month( const std::string& date ) {
    size_t first = date.find_first_of("-");
    size_t second = date.find_first_of("-",first+1);
    return atoi((date.substr(first+1,second-first-1)).c_str());
}

unsigned int day( const std::string& date ) {
    size_t first = date.find_first_of("-");
    size_t second = date.find_first_of("-",first+1);
    return atoi((date.substr(second+1,date.length()-second-1)).c_str());
}

unsigned int year( const std::string& date ) {
    size_t first = date.find_first_of("-");
    size_t second = date.find_first_of("-",first+1);
    size_t third = date.find_first_of("-",second+1);
    return atoi((date.substr(third+1,date.length()-third-1)).c_str());
}

long long parse_date( const std::string& date ) {
    size_t first = date.find_first_of("-");
    size_t second = date.find_first_of("-",first+1);
    int year = atoi((date.substr(0,first)).c_str());
    int month = atoi((date.substr(first+1,second-first-1)).c_str());
    int day = atoi((date.substr(second+1,date.length()-second-1)).c_str());
    boost::posix_time::ptime current(boost::gregorian::date(year, month, day));
    boost::posix_time::ptime original(boost::gregorian::date(1970, 1, 1));
    boost::posix_time::time_duration diff = current - original;
    return diff.total_milliseconds();
}

long long parse_date(int year, int month, int day) {
    boost::posix_time::ptime current(boost::gregorian::date(year, month, day));
    boost::posix_time::ptime original(boost::gregorian::date(1970, 1, 1));
    boost::posix_time::time_duration diff = current - original;
    return diff.total_milliseconds();
}


}
}

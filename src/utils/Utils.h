
#ifndef SPARKSEE_UTILS_INCLUDE_H
#define SPARKSEE_UTILS_INCLUDE_H

#include "GroupBy.h"
#include <gdb/common.h>
#include <boostPatch/property_tree/ptree.hpp>
#include <boost/scoped_ptr.hpp>
#include "gdb/Sparksee.h"
#include "gdb/Database.h"
#include <gdb/Session.h>
#include <gdb/Graph.h>
#include <gdb/Objects.h>
#include <gdb/ObjectsIterator.h>

namespace sparksee {

namespace gdb {
class Objects;
class Graph;
class Session;
class Value;
enum Condition;
enum EdgesDirection;
}


namespace utils {

  typedef boost::scoped_ptr<gdb::Sparksee> SparkseePtr;
  typedef boost::scoped_ptr<gdb::Database> DatabasePtr;
typedef boost::scoped_ptr<gdb::Session> SessionPtr;
typedef boost::scoped_ptr<gdb::Graph> GraphPtr;
typedef boost::scoped_ptr<gdb::Objects> ObjectsPtr; 
typedef boost::scoped_ptr<gdb::ObjectsIterator> ObjectsIteratorPtr; 

template <typename T>
void top(std::vector<T> &list, int limit);

template <typename T>
bool ascending(T lhs, T rhs);

template <typename T>
bool descending(T lhs, T rhs);

gdb::Objects *k_hop(gdb::Session &sess, const gdb::Graph &graph,
                    const gdb::oid_t oid, const gdb::type_t edge,
                    const gdb::EdgesDirection dir, const int num_hops,
                    const bool include_src);

gdb::Objects *select(gdb::Session &sess, const gdb::Graph &graph,
                     const gdb::type_t  edge_t, const gdb::EdgesDirection dir,
                     const gdb::attr_t attr_t, gdb::Condition condition, gdb::Value& val );

gdb::Objects *select(gdb::Session &sess, const gdb::Graph &graph,
                     const gdb::type_t  edge_t, const gdb::EdgesDirection dir,
                     const gdb::attr_t attr_t, gdb::Condition condition, gdb::Value& val, gdb::Objects* restriction );


sparksee::utils::GroupBy* group_by( gdb::Session& sess, const gdb::Graph& graph, gdb::Objects* edges, GroupBy::Mode mode );

std::wstring to_wstring( const std::string& s);
std::wstring to_wstring( const char *s);

std::string to_string(const std::wstring &ws);
std::string to_string(const wchar_t ws);
std::string to_string(const long long i);
std::string to_string(const long i);
std::string to_string(const int i);
std::string format_datetime(long long timestamp);
std::string format_date(long long timestamp);

unsigned int month( long long timestamp );
unsigned int day( long long timestamp );
unsigned int year( long long timestamp );

unsigned int month( const std::string& date );
unsigned int day( const std::string& date );
unsigned int year( const std::string& date );

long long parse_date( const std::string& date );
long long parse_date(int year, int month, int day);

}
}

template <typename T>
void sparksee::utils::top(std::vector<T> &list, int limit) {
    if (list.size() > (unsigned int)limit) {
            list.erase(list.begin() + limit, list.end());
        }
}

template <typename T>
bool sparksee::utils::ascending(T lhs, T rhs) {
    return lhs < rhs;
}

template <typename T>
bool sparksee::utils::descending(T lhs, T rhs) {
    return lhs > rhs;
}

#endif

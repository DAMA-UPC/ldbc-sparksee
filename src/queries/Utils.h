
#ifndef UTILS_INCLUDE_H
#define UTILS_INCLUDE_H

#include <gdb/common.h>
#include <boostPatch/property_tree/ptree.hpp>
#include <boost/scoped_ptr.hpp>
#include <gdb/Session.h>
#include <gdb/Graph.h>
#include <gdb/Objects.h>
#include <gdb/ObjectsIterator.h>
#include "TypeCache.h"
#include "../server/utils/DataTypes.h"

#ifndef NO_TRANSACTIONS
#define BEGIN_TRANSACTION \
            sess->Begin();
#define BEGIN_UPDATE_TRANSACTION \
            sess->Begin();
#define COMMIT_TRANSACTION \
            sess->Commit();
#else
#define BEGIN_TRANSACTION
#define BEGIN_UPDATE_TRANSACTION
#define COMMIT_TRANSACTION
#endif

#ifdef CAPTURE_EXCEPTIONS 
#define BEGIN_EXCEPTION \
            try { 
#define END_EXCEPTION \
            } catch ( gdb::Exception& e ) { \
              std::cout << "EXCEPTION WHEN RUNNING QUERY" << std::endl; \
              std::cout << e.Message() << std::endl; \
              exit(1); \
              return datatypes::Buffer(0,0); \
            }
#else
#define BEGIN_EXCEPTION
#define END_EXCEPTION 
#endif

namespace sparksee {

namespace gdb {
class Objects;
class Graph;
class Session;
class Value;
enum Condition;
enum EdgesDirection;
}


namespace snb {
namespace utils {

gdb::Objects* get_tags(long long classtag_oid, gdb::Graph &graph);
gdb::Objects* people_from_country(long long country_oid, gdb::Graph &graph);


char* to_json( boost::property_tree::ptree& ptree );

char* empty_json();

double similarity(gdb::Session& sess, 
                        gdb::Graph& graph, 
                        snb::TypeCache& cache,
                        gdb::oid_t person1_oid, 
                        gdb::oid_t person2_oid);

long long country_date( long long country_id, long long date );

template <typename T>
void write( char** buffer, int &buffer_size, int & position,  T element ) {
  if( (int)(position + sizeof(element)) >= buffer_size ) {
    int new_size = buffer_size > (int)sizeof(element) ? buffer_size*2 : buffer_size*2 + sizeof(element) ;
    char* new_buffer = new char[new_size];
    memcpy(new_buffer, *buffer, buffer_size);
    buffer_size=new_size;
    delete [] *buffer;
    *buffer = new_buffer;
  }
  memcpy(static_cast<void*>(&(*buffer)[position]), &element, sizeof(T));
  position+=sizeof(element);
}

void write_str( char** buffer, int &buffer_size, int & position,  const std::string& str );

unsigned long date(unsigned int year, unsigned int month, unsigned int day);

}
}

}

#endif

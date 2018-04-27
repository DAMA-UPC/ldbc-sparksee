

#include "TypeCache.h"
#include <gdb/Session.h>
#include <gdb/Graph.h>
#include <vector>
#include <cassert>

#define SNBTYPEMACRO(varname, typestring) varname = graph->FindType(typestring);\
            assert(varname != gdb::Type::InvalidType);
#define SNBTYPEATTRIBUTEMACRO(varname, typevarname, attribute)                 \
            varname = graph->FindAttribute(typevarname, attribute);\
            assert(varname != gdb::Attribute::InvalidAttribute);

namespace sparksee {
namespace snb {

TypeCache::TypeCache(gdb::Graph *graph) {
  SNBTYPELIST
  SNBTYPEATTRIBUTELIST

  knows_similarity_t = graph->FindAttribute(knows_t, L"similarity");
  if(knows_similarity_t == gdb::Attribute::InvalidAttribute ) {
    knows_similarity_t = graph->NewAttribute(knows_t, L"similarity", gdb::Double, gdb::Basic);
  }

  post_country_date_t = graph->FindAttribute(post_t, L"countryDate");
  if(post_country_date_t == gdb::Attribute::InvalidAttribute ) {
    post_country_date_t = graph->NewAttribute(post_t, L"countryDate", gdb::Long, gdb::Indexed);
  }

  comment_country_date_t = graph->FindAttribute(comment_t, L"countryDate");
  if(comment_country_date_t == gdb::Attribute::InvalidAttribute ) {
    comment_country_date_t = graph->NewAttribute(comment_t, L"countryDate", gdb::Long, gdb::Indexed);
  }
}

TypeCache *TypeCache::instance_ = NULL;
boost::signals2::mutex TypeCache::mutex_;

TypeCache *TypeCache::instance(gdb::Graph *graph) {
  mutex_.lock();
  if (instance_ == NULL) {
    instance_ = new TypeCache(graph);
  }
  mutex_.unlock();
  return instance_;
}
}
}
#undef SNBYPEMACRO
#undef SNBTYPEATTRIBUTEMACRO

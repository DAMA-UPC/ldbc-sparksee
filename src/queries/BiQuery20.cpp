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
namespace query20 {

struct Key {

  Key(std::string name)
      : tagclass_name(name) {}

    std::string tagclass_name;
    bool operator<(const Key &b) const {
      return tagclass_name < b.tagclass_name;
    }
};

struct Value {
    Value(int count) : post_count(count) {}

    int post_count;
};

struct Result {

    Result(std::string name, int count)
      : key(name), value(count) {}

    Key key;
    Value value;
};

bool compare_result(const Result &lhs, const Result &rhs) {
  if (lhs.value.post_count != rhs.value.post_count) {
      return sparksee::utils::descending(lhs.value.post_count, rhs.value.post_count);
  }
  return sparksee::utils::ascending(lhs.key.tagclass_name, rhs.key.tagclass_name);
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Result &result) {
    gdb::Value val;
    ptree::ptree pt;

    pt.put<std::string>("TagClass.name", result.key.tagclass_name);
    pt.put<int>("postCount", result.value.post_count);

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

datatypes::Buffer Execute(gdb::Session *sess, const char** tagClasses, int numTagClasses, int limit) {
#ifdef VERBOSE
    printf("Bi QUERY20: ");
    if(numTagClasses > 0) {
       printf("%s",tagClasses[0]);
      for (int i = 1; i < numTagClasses; ++i) {
       printf(", %s",tagClasses[i]);
      }
    }
    printf("\n");
#endif

    BEGIN_EXCEPTION

    boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
    snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

    BEGIN_TRANSACTION;

    gdb::Value value;

    std::vector<Result> intermediate_result;
    for (int i = 0; i < numTagClasses; ++i) {
	    boost::scoped_ptr<gdb::Objects> parentClasses(sess->NewObjects());
	    parentClasses->Union(boost::scoped_ptr<gdb::Objects>(graph->Select(cache->tagclass_name_t, gdb::Equal, value.SetString(sparksee::utils::to_wstring(tagClasses[i])))).get()); 

	    int numclasses = 0;
	    do
	    {
		    numclasses = parentClasses->Count();
		    sparksee::utils::ObjectsPtr children(graph->Neighbors(parentClasses.get(), cache->is_subclass_of_t, gdb::Ingoing));
		    parentClasses->Union(children.get());
	    } while(parentClasses->Count() != numclasses); 

	    sparksee::utils::ObjectsPtr filtered_tags(graph->Neighbors(parentClasses.get(), cache->has_type_t, gdb::Ingoing));
	    sparksee::utils::ObjectsPtr messages_with_tags(graph->Neighbors(filtered_tags.get(), cache->has_tag_t, gdb::Ingoing));
	    boost::scoped_ptr<gdb::Objects> posts(graph->Select(cache->post_t));
	    posts->Intersection(messages_with_tags.get());
	    boost::scoped_ptr<gdb::Objects> comments(graph->Select(cache->comment_t));
	    comments->Intersection(messages_with_tags.get());

	    int count = 0;
	    {
		    boost::scoped_ptr<gdb::ObjectsIterator> iter_messages(posts->Iterator());
		    while( iter_messages->HasNext()) {
			    gdb::oid_t  message_oid = iter_messages->Next();
			    boost::scoped_ptr<gdb::Objects> tags(graph->Neighbors(message_oid, cache->has_tag_t, gdb::Outgoing));
			    boost::scoped_ptr<gdb::ObjectsIterator> iter_tags(tags->Iterator());
			    while(iter_tags->HasNext()) {
				    gdb::oid_t tag_oid = iter_tags->Next();
				    if(filtered_tags->Exists(tag_oid)) {
					    graph->GetAttribute(boost::scoped_ptr<gdb::Objects>(graph->Neighbors(tag_oid, cache->has_type_t, gdb::Outgoing))->Any(), cache->tagclass_name_t, value);
					    std::string tag_class_name = sparksee::utils::to_string(value.GetString());
					    count++;
				    }
			    }
		    }
	    }

	    {
		    boost::scoped_ptr<gdb::ObjectsIterator> iter_messages(comments->Iterator());
		    while( iter_messages->HasNext()) {
			    gdb::oid_t  message_oid = iter_messages->Next();
			    boost::scoped_ptr<gdb::Objects> tags(graph->Neighbors(message_oid, cache->has_tag_t, gdb::Outgoing));
			    boost::scoped_ptr<gdb::ObjectsIterator> iter_tags(tags->Iterator());
			    while(iter_tags->HasNext()) {
				    gdb::oid_t tag_oid = iter_tags->Next();
				    if(filtered_tags->Exists(tag_oid)) {
					    graph->GetAttribute(boost::scoped_ptr<gdb::Objects>(graph->Neighbors(tag_oid, cache->has_type_t, gdb::Outgoing))->Any(), cache->tagclass_name_t, value);
					    std::string tag_class_name = sparksee::utils::to_string(value.GetString());
					    count++;
				    }
			    }
		    }
	    }

	    intermediate_result.push_back(Result(std::string(tagClasses[i]), count));
    }

    std::sort(intermediate_result.begin(), intermediate_result.end(), compare_result);
    ptree::ptree pt = Project(*graph, *cache, intermediate_result);
    COMMIT_TRANSACTION

#ifdef VERBOSE
	    printf("EXIT BI QUERY20: %lu\n", intermediate_result.size());
#endif
    char* ret = snb::utils::to_json(pt);
    return datatypes::Buffer(ret, strlen(ret));

    END_EXCEPTION
}
}
}

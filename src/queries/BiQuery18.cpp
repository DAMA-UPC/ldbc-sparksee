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
namespace query18 {

struct Result {

  Result(int posts, int persons) 
    : num_posts(posts), num_persons(persons) {}

  static Result create(std::map<int, int>::value_type map_pair) {
    return Result(map_pair.first, map_pair.second);
  }

  int num_posts;
  int num_persons;
};

bool compare_result(const Result &lhs, const Result &rhs) {
  if (lhs.num_persons != rhs.num_persons) {
    return sparksee::utils::descending(lhs.num_persons, rhs.num_persons);
  }

  return sparksee::utils::descending(lhs.num_posts, rhs.num_posts);
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache, const Result &result) {
  gdb::Value val;
  ptree::ptree pt;

  pt.put<int>("postCount", result.num_posts);
  pt.put<int>("personCount", result.num_persons);

  return pt;
}

static ptree::ptree Project(const gdb::Graph &graph,
                            const snb::TypeCache &cache,
                            const std::vector<Result> &results, int limit) {
  ptree::ptree pt;
  int count = 0;
  for (std::vector<Result>::const_iterator it = results.begin();
       it != results.end() /*&& count < limit*/; ++it, ++count) {
    pt.push_back(std::make_pair("", Project(graph, cache, *it)));
  }
  return pt;
}

datatypes::Buffer Execute(gdb::Session *sess, long long date, int num_languages, const char** languages, unsigned int threshold,  int limit) {
#ifdef VERBOSE
  printf("Bi QUERY18: %llu\n", date);
#endif

  BEGIN_EXCEPTION

    boost::scoped_ptr<gdb::Graph> graph(sess->GetGraph());
  snb::TypeCache *cache = snb::TypeCache::instance(graph.get());

  BEGIN_TRANSACTION;

  gdb::Value value;
  value.SetTimestamp(date);

  boost::scoped_ptr<gdb::Objects> persons(graph->Select(cache->person_t));
  boost::scoped_ptr<gdb::Objects> posts(graph->Select(cache->post_creation_date_t, gdb::GreaterThan, value));
  boost::scoped_ptr<gdb::Objects> qualifying_posts(sess->NewObjects());
  boost::scoped_ptr<gdb::ObjectsIterator> iter_posts(posts->Iterator());
  while(iter_posts->HasNext()) {
    gdb::oid_t next_post = iter_posts->Next();
    graph->GetAttribute(next_post,cache->post_language_t, value);
    gdb::Value content;
    gdb::Value image;
    graph->GetAttribute(next_post, cache->post_content_t, content);
    graph->GetAttribute(next_post, cache->post_image_file_t, image);
    if(image.IsNull() && !value.IsNull() && value.GetString().length() != 0 && !content.IsNull() &&  content.GetString().length() != 0 && content.GetString().length() < threshold) {
      for (int i = 0; i < num_languages; ++i) {
        if(value.GetString().compare( sparksee::utils::to_wstring(std::string(languages[i])) ) == 0) {
          qualifying_posts->Add(next_post);
          break;
        }
      }
    }
  }

  value.SetTimestamp(date);
  boost::scoped_ptr<gdb::Objects> comments(graph->Select(cache->comment_creation_date_t, gdb::GreaterThan, value));
  boost::scoped_ptr<gdb::Objects> qualifying_comments(sess->NewObjects());
  boost::scoped_ptr<gdb::ObjectsIterator> iter_comments(comments->Iterator());
  while(iter_comments->HasNext()) {
    gdb::oid_t next_comment = iter_comments->Next();
    gdb::Value content;
    gdb::Value image;
    graph->GetAttribute(next_comment, cache->comment_content_t, content);
    if(!content.IsNull() && content.GetString().length() != 0 && content.GetString().length() < threshold) {

      gdb::oid_t initiator = next_comment;
      while(true) {
        initiator = boost::scoped_ptr<gdb::Objects>(graph->Neighbors(initiator,cache->reply_of_t,gdb::Outgoing))->Any();
        if(graph->GetObjectType(initiator) == cache->post_t) {
          break;
        }
      }
      gdb::Value language;
      graph->GetAttribute(initiator,cache->post_language_t, language);
      for (int i = 0; i < num_languages; ++i) {
        if(language.GetString().compare( sparksee::utils::to_wstring(std::string(languages[i]))) == 0) {
          qualifying_comments->Add(next_comment);
          break;
        }
      }
    }
  }


  boost::scoped_ptr<gdb::Objects> observed_persons(sess->NewObjects());
  std::map<gdb::oid_t, int> messages_per_person;
  boost::scoped_ptr<gdb::ObjectsIterator> iter_qposts(qualifying_posts->Iterator());
  while(iter_qposts->HasNext()) {
    gdb::oid_t next_post = iter_qposts->Next();
    boost::scoped_ptr<gdb::Objects> creator(graph->Neighbors(next_post,cache->post_has_creator_t, gdb::Outgoing));
    gdb::oid_t creator_oid = creator->Any();
    messages_per_person[creator_oid]++;
    observed_persons->Add(creator_oid);
  }

  boost::scoped_ptr<gdb::ObjectsIterator> iter_qcomments(qualifying_comments->Iterator());
  while(iter_qcomments->HasNext()) {
    gdb::oid_t next_comment = iter_qcomments->Next();
    boost::scoped_ptr<gdb::Objects> creator(graph->Neighbors(next_comment,cache->comment_has_creator_t, gdb::Outgoing));
    gdb::oid_t creator_oid = creator->Any();
    messages_per_person[creator_oid]++;
    observed_persons->Add(creator_oid);
  }

  std::map<int, int> hist;
  for(std::map<gdb::oid_t, int>::iterator it = messages_per_person.begin(); 
      it != messages_per_person.end();
      ++it) {
    hist[it->second]++;
  }
  hist[0] = graph->GetType(cache->person_t)->GetNumObjects() - observed_persons->Count();

  std::vector<Result> intermediate_result;
  intermediate_result.reserve(hist.size());
  std::transform(hist.begin(), hist.end(), std::back_inserter(intermediate_result), &Result::create);
  std::sort(intermediate_result.begin(), intermediate_result.end(), compare_result);
  ptree::ptree pt = Project(*graph, *cache, intermediate_result, limit);
  COMMIT_TRANSACTION

#ifdef VERBOSE
    printf("EXIT BI QUERY18: %llu %lu\n", date, intermediate_result.size());
#endif
  char* ret = snb::utils::to_json(pt);
  return datatypes::Buffer(ret, strlen(ret));

  END_EXCEPTION
}
}
}

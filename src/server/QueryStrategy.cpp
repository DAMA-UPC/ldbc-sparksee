#include "QueryStrategy.h"
#include "QueryStrategy/RoundRobin.h"
#include "QueryStrategy/Remote.h"
#include <string>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "server/QueryStatistics.h"
#include "server/utils/Timestamps.h"

namespace ldbc_server {
QueryStrategy *QueryStrategy::Factory(sparksee::Database *database,
                                      const char *name, const char *json_file,
                                      unsigned int num_queries,
                                      unsigned int port,
                                      unsigned int shutdown_port) {
  if (strcmp(name, "remote") == 0) {
    return new query_strategy::Remote(database, port, shutdown_port);
  }
  return new query_strategy::RoundRobin(database, json_file, num_queries);
}

boost::property_tree::ptree QueryStrategy::read_json(const char *json_file) {
  boost::property_tree::ptree tree;
  boost::property_tree::read_json(json_file, tree);
  return tree;
}

boost::property_tree::ptree
QueryStrategy::read_json(std::stringstream &stream) {
  boost::property_tree::ptree tree;
  boost::property_tree::read_json(stream, tree);
  return tree;
}

unsigned int QueryStrategy::parse_query_id(const std::string &element) {
  if (element.substr(0, 5) == "query") {
    return static_cast<unsigned int>(std::stoi(element.substr(5)) + query_range::kMin - 1);
  } else if (element.substr(0, 6) == "update") {
    return static_cast<unsigned int>(std::stoi(element.substr(6)) + update_range::kMin - 1);
  } else if (element.substr(0, 5) == "short") {
    return static_cast<unsigned int>(std::stoi(element.substr(5)) + short_range::kMin - 1);
  } else if (element.substr(0, 2) == "bi") {
    return static_cast<unsigned int>(std::stoi(element.substr(2)) + bi_range::kMin - 1);
  }

  return static_cast<unsigned int>(QueryId::kNumQueries);
}

unsigned int QueryStrategy::get_first_id(boost::property_tree::ptree tree) {
  for (const auto &queries : tree) { // query lvl
    return parse_query_id(queries.first);
  }
  return QueryId::kNumQueries;
}

void QueryStrategy::search_values(boost::property_tree::ptree tree, std::vector<std::string> &param_values) {
  if (tree.size() == 0) {
    param_values.push_back(tree.data());
    return;
  }
  for(const auto &nodes : tree) {
    search_values(nodes.second, param_values);
  }
}

void QueryStrategy::populate_parameter_list(boost::property_tree::ptree tree) {
  parameters_mutex_.lock();
  if (parameters_.size() != QueryId::kNumQueries) {
    parameters_.resize(kNumQueries - parameters_.size());
  }
  for (const auto &queries : tree) { // query lvl
    unsigned int query_id = parse_query_id(queries.first);
    if (query_id >= QueryId::kNumQueries) {
      continue;
    }
    for (const auto &list : queries.second) { // list lvl
      std::map<std::string, std::vector<std::string>> parameters;
      std::vector<std::string> query_id_array;
      query_id_array.push_back(std::to_string(query_id));
      parameters.insert(std::make_pair("queryId",query_id_array));
      for (const auto &param : list.second) { // param lvl
        std::vector<std::string> param_values;
        search_values(param.second, param_values);
        parameters.insert(
            std::make_pair(param.first.data(), param_values));
      }
      parameters_[query_id].emplace_back(parameters);
    }
  }
  parameters_mutex_.unlock();
}

const char *QueryStrategy::copy_string(
    std::map<std::string, std::vector<std::string>> &parameters,
    std::string name, size_t index) {
  std::string &tmp = parameters[name][index];
  char* raw = new char[tmp.length() + 1];
  std::memcpy(raw, tmp.c_str(), tmp.length() + 1);
  return raw;
}

Worker::Task
QueryStrategy::function_factory(std::map<std::string, std::vector<std::string>> parameters,
                                std::function<void(const datatypes::Buffer &)> callback) {

  unsigned long dispatched = timestamp::current_micros();
  unsigned long id = static_cast<unsigned long>(std::stol(parameters["queryId"][0].c_str()));
  switch (id) {
  case kInteractive1: {
    long long person_id = std::stol(parameters["id"][0].c_str());
    const char *name = copy_string(parameters, "name", 0);
    unsigned int limit = static_cast<unsigned int>(std::stoi(parameters["limit"][0]));
    return std::make_tuple(kInteractive1, dispatched,
                           std::bind(interactive::query1::Execute, std::placeholders::_1,
                                     person_id, name, limit),
                           callback, std::bind([=](){delete[] name;}));
  } break;
  case kInteractive2: {
    long long person_id = std::stol(parameters["id"][0]);
    long long max_date = std::stol(parameters["date"][0]);
    unsigned int limit = static_cast<unsigned int>(std::stoi(parameters["limit"][0]));
    return std::make_tuple(kInteractive2, dispatched,
                           std::bind(interactive::query2::Execute, std::placeholders::_1,
                                     person_id, max_date, limit),
                           callback, std::bind([](){}));
  } break;
  case kInteractive3: {
    long long person_id = std::stol(parameters["id"][0]);
    const char *country1 = copy_string(parameters, "country1", 0);
    const char *country2 = copy_string(parameters, "country2", 0);
    long long date = std::stol(parameters["date"][0]);
    unsigned int days = static_cast<unsigned int>(std::stoi(parameters["duration"][0]));
    unsigned int limit = static_cast<unsigned int>(std::stoi(parameters["limit"][0]));
    return std::make_tuple(kInteractive3, dispatched,
                           std::bind(interactive::query3::Execute, std::placeholders::_1,
                                     person_id, country1,
                                     country2, date, days, limit),
                           callback, std::bind([=]() {
                             delete[] country1;
                             delete[] country2;
                           }));
  } break;
  case kInteractive4: {
    long long person_id = std::stol(parameters["id"][0]);
    long long start_date = std::stol(parameters["date"][0]);
    unsigned int days = static_cast<unsigned int>(std::stoi(parameters["days"][0]));
    unsigned int limit = static_cast<unsigned int>(std::stoi(parameters["limit"][0]));
    return std::make_tuple(kInteractive4, dispatched,
                           std::bind(interactive::query4::Execute, std::placeholders::_1,
                                     person_id, start_date, days, limit),
                           callback, std::bind([](){}));
  } break;
  case kInteractive5: {
    long long person_id = std::stol(parameters["id"][0]);
    long long min_date = std::stol(parameters["date"][0]);
    unsigned int limit = static_cast<unsigned int>(std::stoi(parameters["limit"][0]));
    return std::make_tuple(kInteractive5, dispatched,
                           std::bind(interactive::query5::Execute, std::placeholders::_1,
                                     person_id, min_date, limit),
                           callback, std::bind([](){}));
  } break;
  case kInteractive6: {
    long long person_id = std::stol(parameters["id"][0]);
    const char *tag = copy_string(parameters, "tag", 0);
    unsigned int limit = static_cast<unsigned int>(std::stoi(parameters["limit"][0]));
    return std::make_tuple(kInteractive6, dispatched,
                           std::bind(interactive::query6::Execute, std::placeholders::_1,
                                     person_id, tag, limit),
                           callback, std::bind([=](){delete[] tag;}));
  } break;
  case kInteractive7: {
    long long person_id = std::stol(parameters["id"][0]);
    unsigned int limit = static_cast<unsigned int>(std::stoi(parameters["limit"][0]));
    return std::make_tuple(
        kInteractive7, dispatched,
        std::bind(interactive::query7::Execute, std::placeholders::_1, person_id, limit),
        callback, std::bind([](){}));
  } break;
  case kInteractive8: {
    long long person_id = std::stol(parameters["id"][0]);
    unsigned int limit = static_cast<unsigned int>(std::stoi(parameters["limit"][0]));
    return std::make_tuple(
        kInteractive8, dispatched,
        std::bind(interactive::query8::Execute, std::placeholders::_1, person_id, limit),
        callback, std::bind([](){}));
  } break;
  case kInteractive9: {
    long long person_id = std::stol(parameters["id"][0]);
    long long max_date = std::stol(parameters["date"][0]);
    unsigned int limit = static_cast<unsigned int>(std::stoi(parameters["limit"][0]));
    return std::make_tuple(kInteractive9, dispatched,
                           std::bind(interactive::query9::Execute, std::placeholders::_1,
                                     person_id, max_date, limit),
                           callback, std::bind([](){}));
  } break;
  case kInteractive10: {
    long long person_id = std::stol(parameters["id"][0]);
    unsigned int month = static_cast<unsigned int>(std::stoi(parameters["month"][0]));
    unsigned int limit = static_cast<unsigned int>(std::stoi(parameters["limit"][0]));
    return std::make_tuple(kInteractive10, dispatched,
                           std::bind(interactive::query10::Execute, std::placeholders::_1,
                                     person_id, month, limit),
                           callback, std::bind([](){}));
  } break;
  case kInteractive11: {
    long long person_id = std::stol(parameters["id"][0]);
    const char *country = copy_string(parameters, "country", 0);
    int year = std::stoi(parameters["year"][0]);
    unsigned int limit = static_cast<unsigned int>(std::stoi(parameters["limit"][0]));
    return std::make_tuple(kInteractive11, dispatched,
                           std::bind(interactive::query11::Execute, std::placeholders::_1,
                                     person_id, country, year, limit),
                           callback, std::bind([=](){delete[] country;}));
  } break;
  case kInteractive12: {
    long long person_id = std::stol(parameters["id"][0]);
    const char *tag_class = copy_string(parameters, "tagclass", 0);
    unsigned int limit = static_cast<unsigned int>(std::stoi(parameters["limit"][0]));
    return std::make_tuple(kInteractive12, dispatched,
                           std::bind(interactive::query12::Execute, std::placeholders::_1,
                                     person_id, tag_class, limit),
                           callback, std::bind([=](){delete[] tag_class;}));
  } break;
  case kInteractive13: {
    long person1 = std::stol(parameters["id1"][0]);
    long person2 = std::stol(parameters["id2"][0]);
    return std::make_tuple(kInteractive13, dispatched,
                           std::bind(interactive::query13::Execute, std::placeholders::_1, person1, person2),
                           callback, std::bind([](){}));
  } break;
  case kInteractive14: {
    long person1 = std::stol(parameters["id1"][0]);
    long person2 = std::stol(parameters["id2"][0]);
    return std::make_tuple(kInteractive14, dispatched,
                           std::bind(interactive::query14::Execute, std::placeholders::_1, person1, person2),
                           callback, std::bind([](){}));
  } break;
  case kUpdate1: {
    long long person = std::stol(parameters["id"][0]);
    const char *first_name = copy_string(parameters, "firstName", 0);
    const char *last_name = copy_string(parameters, "lastName", 0);
    const char *gender = copy_string(parameters,"gender", 0);
    long long birthday = std::stol(parameters["birthday"][0]);
    long long creation_date = std::stol(parameters["date"][0]);
    const char *ip = copy_string(parameters, "ip", 0);
    const char *browser = copy_string(parameters, "browser", 0);
    long long city = std::stol(parameters["cityId"][0]);
    int num_emails = std::stoi(parameters["numEmails"][0]);
    int num_languages = std::stoi(parameters["numLanguages"][0]);
    int num_interests = std::stoi(parameters["numInterests"][0]);
    int num_works = std::stoi(parameters["numWorkAt"][0]);
    int num_studies = std::stoi(parameters["numStudyAt"][0]);
    const char **emails = new const char*[num_emails];
    for (size_t i = 0; i < static_cast<size_t>(num_emails); ++i) {
      emails[i] = copy_string(parameters, "emails", i);
    }
    const char **languages = new const char*[num_languages];
    for (size_t i = 0; i < static_cast<size_t>(num_languages); ++i) {
      languages[i] = copy_string(parameters, "languages", i);
    }
    long long *interests = new long long[num_interests];
    std::vector<std::string> &values = parameters["interests"];
    for (size_t i = 0; i < static_cast<size_t>(num_interests); ++i) {
      interests[i] = std::stol(values[i].c_str());
    }
    long long *work = new long long[num_works*2];
    values = parameters["workAt"];
    for (size_t i = 0; i < static_cast<size_t>(num_works); ++i) {
      work[2*i] = std::stol(values[2*i]);
      work[2*i+1] = std::stol(values[2*i+1]);
    }
    long long *study = new long long[num_studies*2];
    values = parameters["studyAt"];
    for (size_t i = 0; i < static_cast<size_t>(num_studies); ++i) {
      study[2*i] = std::stol(values[2*i]);
      study[2*i+1] = std::stol(values[2*i+1]);
    }
    return std::make_tuple(kUpdate1, dispatched,
                           std::bind(interactive::update1::Execute, std::placeholders::_1, person,
                                     first_name, last_name, gender,
                                     birthday, creation_date, ip, browser,
                                     city, num_emails, emails, num_languages, languages,
                                     num_interests, interests, num_works, work,
                                     num_studies, study),
                           callback, std::bind([=](){
                                                    delete[] first_name;
                                                    delete[] last_name;
                                                    delete[] gender;
                                                    delete[] ip;
                                                    delete[] browser;
                                                    for (size_t i = 0; i < static_cast<size_t>(num_emails); ++i) {
                                                      delete[] emails[i];
                                                    }
                                                    delete[] emails;
                                                    for (size_t i = 0; i < static_cast<size_t>(num_languages); ++i) {
                                                      delete[] languages[i];
                                                    }
                                                    delete[] languages;
                                                    delete[] interests;
                                                    delete[] work;
                                                    delete[] study;})
                           );
  } break;
  case kUpdate2: {
    long long person = std::stol(parameters["id"][0]);
    long long comment = std::stol(parameters["postId"][0]);
    long long creation_date = std::stol(parameters["date"][0]);
    return std::make_tuple(kUpdate2, dispatched,
                           std::bind(interactive::update2::Execute, std::placeholders::_1, person,
                                     comment, creation_date),
                           callback, std::bind([](){}));
  } break;
  case kUpdate3: {
    long long person = std::stol(parameters["id"][0]);
    long long comment = std::stol(parameters["commentId"][0]);
    long long creation_date = std::stol(parameters["date"][0]);
    return std::make_tuple(kUpdate3, dispatched,
                           std::bind(interactive::update3::Execute, std::placeholders::_1, person,
                                     comment, creation_date),
                           callback, std::bind([](){}));
  } break;
  case kUpdate4: {
    long long forum = std::stol(parameters["id"][0]);
    const char *title = copy_string(parameters, "title", 0);
    long long creation_date = std::stol(parameters["date"][0]);
    long long person = std::stol(parameters["personId"][0]);
    int num_tags = std::stoi(parameters["numTags"][0]);
    long long *tags = new long long[num_tags];
    std::vector<std::string> &values = parameters["tags"];
    for (size_t i = 0; i < static_cast<size_t>(num_tags); ++i) {
      tags[i] = std::stol(values[i]);
    }
    return std::make_tuple(kUpdate4, dispatched,
                           std::bind(interactive::update4::Execute, std::placeholders::_1, forum,
                                     title, creation_date, person, num_tags, tags),
                           callback, std::bind([=](){
                                                    delete[] title;
                                                    delete[] tags;})
                           );
  } break;
  case kUpdate5: {
    long long person = std::stol(parameters["id"][0]);
    long long forum = std::stol(parameters["forumId"][0]);
    long long join_date = std::stol(parameters["date"][0]);
    return std::make_tuple(kUpdate5, dispatched,
                           std::bind(interactive::update5::Execute, std::placeholders::_1, person,
                                     forum, join_date),
                           callback, std::bind([](){}));
  } break;
  case kUpdate6: {
    long long post = std::stol(parameters["id"][0]);
    const char *image = copy_string(parameters, "image", 0);
    long long creation_date = std::stol(parameters["date"][0]);
    const char *ip = copy_string(parameters, "ip", 0);
    const char *browser = copy_string(parameters, "browser", 0);
    const char *language = copy_string(parameters, "language", 0);
    const char *content = copy_string(parameters, "content", 0);
    int length = std::stoi(parameters["length"][0]);
    long long person = std::stol(parameters["creatorId"][0]);
    long long forum = std::stol(parameters["forumId"][0]);
    long long place = std::stol(parameters["locationId"][0]);
    int num_tags = std::stoi(parameters["numTags"][0]);
    long long *tags = new long long[num_tags];
    std::vector<std::string> &values = parameters["tags"];
    for (size_t i = 0; i < static_cast<size_t>(num_tags); ++i) {
      tags[i] = std::stol(values[i]);
    }
    return std::make_tuple(kUpdate6, dispatched,
                           std::bind(interactive::update6::Execute, std::placeholders::_1, post,
                                     image, creation_date, ip, browser,
                                     language, content, length, person,
                                     forum, place, num_tags, tags),
                           callback, std::bind([=](){
                                                   delete[] image;
                                                   delete[] ip;
                                                   delete[] browser;
                                                   delete[] language;
                                                   delete[] content;
                                                   delete[] tags;})
                           );
  } break;
  case kUpdate7: {
    long long comment = std::stol(parameters["id"][0]);
    long long creation_date = std::stol(parameters["date"][0]);
    const char *ip = copy_string(parameters, "ip", 0);
    const char *browser = copy_string(parameters, "browser", 0);
    const char *content = copy_string(parameters, "content", 0);
    int length = std::stoi(parameters["length"][0]);
    long long person = std::stol(parameters["creatorId"][0]);
    long long place = std::stol(parameters["locationId"][0]);
    long long reply_post = std::stol(parameters["replyPost"][0]);
    long long reply_comment = std::stol(parameters["replyComment"][0]);
    int num_tags = std::stoi(parameters["numTags"][0]);
    long long *tags = new long long[num_tags];
    std::vector<std::string> &values = parameters["tags"];
    for (size_t i = 0; i < static_cast<size_t>(num_tags); ++i) {
      tags[i] = std::stol(values[i]);
    }
    return std::make_tuple(kUpdate7, dispatched,
                           std::bind(interactive::update7::Execute, std::placeholders::_1, comment,
                                     creation_date, ip, browser,
                                     content, length, person, place,
                                     reply_post, reply_comment, num_tags, tags),
                           callback, std::bind([=](){
                                                   delete[] ip;
                                                   delete[] browser;
                                                   delete[] content;
                                                   delete[] tags;})
                           );
  } break;
  case kUpdate8: {
    long long person1 = std::stol(parameters["id1"][0]);
    long long person2 = std::stol(parameters["id2"][0]);
    long long creation_date = std::stol(parameters["date"][0]);
    return std::make_tuple(kUpdate8, dispatched,
                           std::bind(interactive::update8::Execute, std::placeholders::_1, person1,
                                     person2, creation_date),
                           callback, std::bind([](){}));
  } break;  
  case kShort1: {
    long person = std::stol(parameters["id"][0]);
    return std::make_tuple(kShort1, dispatched,
                           std::bind(interactive::short1::Execute, std::placeholders::_1, person),
                           callback, std::bind([](){}));
  } break;
  case kShort2: {
    long person = std::stol(parameters["id"][0]);
    return std::make_tuple(kShort2, dispatched,
                           std::bind(interactive::short2::Execute, std::placeholders::_1, person),
                           callback, std::bind([](){}));
  } break;
  case kShort3: {
    long person = std::stol(parameters["id"][0]);
    return std::make_tuple(kShort3, dispatched,
                           std::bind(interactive::short3::Execute, std::placeholders::_1, person),
                           callback, std::bind([](){}));
  } break;
  case kShort4: {
    long message = std::stol(parameters["id"][0]);
    return std::make_tuple(kShort4, dispatched,
                           std::bind(interactive::short4::Execute, std::placeholders::_1, message),
                           callback, std::bind([](){}));
  } break;
  case kShort5: {
    long message = std::stol(parameters["id"][0]);
    return std::make_tuple(kShort5, dispatched,
                           std::bind(interactive::short5::Execute, std::placeholders::_1, message),
                           callback, std::bind([](){}));
  } break;
  case kShort6: {
    long message = std::stol(parameters["id"][0]);
    return std::make_tuple(kShort6, dispatched,
                           std::bind(interactive::short6::Execute, std::placeholders::_1, message),
                           callback, std::bind([](){}));
  } break;
  case kShort7: {
    long message = std::stol(parameters["id"][0]);
    return std::make_tuple(kShort7, dispatched,
                           std::bind(interactive::short7::Execute, std::placeholders::_1, message),
                           callback, std::bind([](){}));
  } break;
  case kBi1: {
    long long date = std::stol(parameters["date"][0]);
    return std::make_tuple(kBi1, dispatched,
                           std::bind(bi::query1::Execute, std::placeholders::_1, date),
                           callback, std::bind([](){}));
             } break;
  case kBi2: {
    long long date1 = std::stol(parameters["date1"][0]);
    long long date2 = std::stol(parameters["date2"][0]);
    const char* country1 = copy_string(parameters,"country1",0);
    const char* country2 = copy_string(parameters,"country2",0);
    unsigned int limit = static_cast<unsigned int>(std::stoi(parameters["limit"][0]));
    return std::make_tuple(kBi2, dispatched,
                           std::bind(bi::query2::Execute, std::placeholders::_1, date1, date2, country1, country2, limit),
                           callback, std::bind([=](){
                                               delete [] country1;
                                               delete [] country2;
                             }));
  } break;

  case kBi3: {
    unsigned int year = std::stoi(parameters["year"][0]);
    unsigned int month = std::stoi(parameters["month"][0]);
    unsigned int limit = static_cast<unsigned int>(std::stoi(parameters["limit"][0]));
    return std::make_tuple(kBi3, dispatched,
                           std::bind(bi::query3::Execute, std::placeholders::_1, year, month, limit),
                           callback, std::bind([=](){}));
  } break;

  case kBi4: {
    const char* tagClass = copy_string(parameters, "tagClass", 0);
    const char* country = copy_string(parameters, "country", 0);
    unsigned int limit = static_cast<unsigned int>(std::stoi(parameters["limit"][0]));
    return std::make_tuple(kBi4, dispatched,
                           std::bind(bi::query4::Execute, std::placeholders::_1, tagClass, country, limit),
                           callback, std::bind([=](){
                             delete [] tagClass;
                             delete [] country;
                             }));
  } break;

  case kBi5: {
    const char* country = copy_string(parameters, "country", 0);
    unsigned int limit = static_cast<unsigned int>(std::stoi(parameters["limit"][0]));
    return std::make_tuple(kBi5, dispatched,
                           std::bind(bi::query5::Execute, std::placeholders::_1, country, limit),
                           callback, std::bind([=](){
                             delete [] country;
                             }));
  } break;
  case kBi6: {
    const char* tag = copy_string(parameters, "tag", 0);
    unsigned int limit = static_cast<unsigned int>(std::stoi(parameters["limit"][0]));
    return std::make_tuple(kBi6, dispatched,
                           std::bind(bi::query6::Execute, std::placeholders::_1, tag, limit),
                           callback, std::bind([=](){
                             delete [] tag;
                             }));
  } break;
  case kBi7: {
    const char* tag = copy_string(parameters, "tag", 0);
    unsigned int limit = static_cast<unsigned int>(std::stoi(parameters["limit"][0]));
    return std::make_tuple(kBi7, dispatched,
                           std::bind(bi::query7::Execute, std::placeholders::_1, tag, limit),
                           callback, std::bind([=](){
                             delete [] tag;
                             }));
  } break;
  case kBi8: {
    const char* tag = copy_string(parameters, "tag", 0);
    unsigned int limit = static_cast<unsigned int>(std::stoi(parameters["limit"][0]));
    return std::make_tuple(kBi8, dispatched,
                           std::bind(bi::query8::Execute, std::placeholders::_1, tag, limit),
                           callback, std::bind([=](){
                             delete [] tag;
                             }));
  } break;
  case kBi9: {
    const char* class1 = copy_string(parameters, "tagClass1", 0);
    const char* class2 = copy_string(parameters, "tagClass2", 0);
    int threshold = std::stoi(parameters["threshold"][0]);
    int limit = std::stoi(parameters["limit"][0]);
    return std::make_tuple(kBi9, dispatched,
                           std::bind(bi::query9::Execute, std::placeholders::_1, class1, class2, threshold, limit),
                           callback, std::bind([=](){
                             delete [] class1;
                             delete [] class2;
                             }));
  } break;
  case kBi10: {
    const char* tag = copy_string(parameters, "tag", 0);
    long long date = std::stol(parameters["date"][0]);
    unsigned int limit = std::stoi(parameters["limit"][0]);
    return std::make_tuple(kBi10, dispatched,
                           std::bind(bi::query10::Execute, std::placeholders::_1, tag, date, limit),
                           callback, std::bind([=](){
                             delete [] tag;
                             }));
  } break;
  case kBi11: {
    const char* country = copy_string(parameters, "country", 0);
    int blacklist_size = std::stoi(parameters["blacklistSize"][0]);
    const char **blacklist = new const char*[blacklist_size];
    for (size_t i = 0; i < static_cast<size_t>(blacklist_size); ++i) {
        blacklist[i] = copy_string(parameters, "blacklist", i);
    }
    int limit = std::stoi(parameters["limit"][0]);
    return std::make_tuple(kBi11, dispatched,
                           std::bind(bi::query11::Execute, std::placeholders::_1, country, blacklist, blacklist_size, limit),
                           callback, std::bind([=](){
                             delete [] country;
                             for( int i = 0; i < blacklist_size; ++i ) {
                              delete [] blacklist[i];
                             }
                             delete [] blacklist;
                             }));
  } break;
  case kBi12: {
    long long date = std::stol(parameters["date"][0]);
    int like_count = std::stoi(parameters["likeThreshold"][0]);
    int limit = std::stoi(parameters["limit"][0]);
    return std::make_tuple(kBi12, dispatched,
                           std::bind(bi::query12::Execute, std::placeholders::_1, date, like_count, limit),
                           callback, std::bind([=](){
                             }));
  } break;
  case kBi13: {
    const char* country = copy_string(parameters, "country", 0);
    int limit = std::stoi(parameters["limit"][0]);
    return std::make_tuple(kBi13, dispatched,
                           std::bind(bi::query13::Execute, std::placeholders::_1, country, limit),
                           callback, std::bind([=](){
                             delete [] country;
                             }));
  } break;
  case kBi14: {
    long long begin = std::stol(parameters["begin"][0]);
    long long end = std::stol(parameters["end"][0]);
    int limit = std::stoi(parameters["limit"][0]);
    return std::make_tuple(kBi14, dispatched,
                           std::bind(bi::query14::Execute, std::placeholders::_1, begin, end, limit),
                           callback, std::bind([=](){
                             }));
  } break;
  case kBi15: {
    const char* country = copy_string(parameters, "country", 0);
    int limit = std::stoi(parameters["limit"][0]);
    return std::make_tuple(kBi15, dispatched,
                           std::bind(bi::query15::Execute, std::placeholders::_1, country, limit),
                           callback, std::bind([=](){
                             delete [] country;
                             }));
  } break;
  case kBi16: {
    long long id = std::stol(parameters["person"][0]);
    const char* country = copy_string(parameters, "country", 0);
    const char* tag_class = copy_string(parameters, "tagClass", 0);
    int min = std::stoi(parameters["min"][0]);
    int max = std::stoi(parameters["max"][0]);
    int limit = std::stoi(parameters["limit"][0]);
    return std::make_tuple(kBi16, dispatched,
                           std::bind(bi::query16::Execute, std::placeholders::_1, id, country, tag_class, min, max, limit),
                           callback, std::bind([=](){
                             delete [] country;
                             delete [] tag_class;
                             }));
  } break;
  case kBi17: {
    const char* country = copy_string(parameters, "country", 0);
    int limit = std::stoi(parameters["limit"][0]);
    return std::make_tuple(kBi17, dispatched,
                           std::bind(bi::query17::Execute, std::placeholders::_1, country, limit),
                           callback, std::bind([=](){
                             delete [] country;
                             }));
  } break;
  case kBi18: {
    long long date = std::stol(parameters["date"][0]);
    int limit = std::stoi(parameters["limit"][0]);
    int languages_size = std::stoi(parameters["numLanguages"][0]);
    const char **languages = new const char*[languages_size];
    for (size_t i = 0; i < static_cast<size_t>(languages_size); ++i) {
        languages[i] = copy_string(parameters, "languages", i);
    }
    int threshold = std::stoi(parameters["threshold"][0]);
    return std::make_tuple(kBi18, dispatched,
                           std::bind(bi::query18::Execute, std::placeholders::_1, date, languages_size, languages, threshold, limit),
                           callback, std::bind([=](){
                             for( int i = 0; i < languages_size; ++i ) {
                              delete [] languages[i];
                             }
                             delete [] languages;
                             }));
  } break;
  case kBi19: {
    long long date = std::stol(parameters["date"][0]);
    const char* tagClass1 = copy_string(parameters, "tagClass1", 0);
    const char* tagClass2 = copy_string(parameters, "tagClass2", 0);
    int limit = std::stoi(parameters["limit"][0]);
    return std::make_tuple(kBi19, dispatched,
                           std::bind(bi::query19::Execute, std::placeholders::_1, date, tagClass1, tagClass2, limit),
                           callback, std::bind([=](){
                             delete [] tagClass1;
                             delete [] tagClass2;
                             }));
  } break;
  case kBi20: {
    int numClasses = std::stoi(parameters["numClasses"][0]);
    const char **classes = new const char*[numClasses];
    for (size_t i = 0; i < static_cast<size_t>(numClasses); ++i) {
      classes[i] = copy_string(parameters, "classes", i);
    }
    int limit = std::stoi(parameters["limit"][0]);
    return std::make_tuple(kBi20, dispatched,
                           std::bind(bi::query20::Execute, std::placeholders::_1, classes, numClasses, limit),
                           callback, std::bind([=](){
                             for (int i = 0; i < numClasses; ++i) {
                               delete [] classes[i];
                             }
                             delete [] classes;
                             }));
  } break;
  case kBi21: {
    long long date = std::stol(parameters["date"][0]);
    const char* country = copy_string(parameters, "country", 0);
    int limit = std::stoi(parameters["limit"][0]);
    return std::make_tuple(kBi21, dispatched,
                           std::bind(bi::query21::Execute, std::placeholders::_1, country, date, limit),
                           callback, std::bind([=](){
                             delete [] country;
                             }));
  } break;
  case kBi22: {
    const char* country1 = copy_string(parameters, "country1", 0);
    const char* country2 = copy_string(parameters, "country2", 0);
    int limit = std::stoi(parameters["limit"][0]);
    return std::make_tuple(kBi22, dispatched,
                           std::bind(bi::query22::Execute, std::placeholders::_1, country1, country2, limit),
                           callback, std::bind([=](){
                             delete [] country1;
                             delete [] country2;
                             }));
  } break;
  case kBi23: {
    const char* country = copy_string(parameters, "country", 0);
    int limit = std::stoi(parameters["limit"][0]);
    return std::make_tuple(kBi23, dispatched,
                           std::bind(bi::query23::Execute, std::placeholders::_1, country, limit),
                           callback, std::bind([=](){
                             delete [] country;
                             }));
  } break;
  case kBi24: {
    const char* tagClass = copy_string(parameters, "tagClass", 0);
    int limit = std::stoi(parameters["limit"][0]);
    return std::make_tuple(kBi24, dispatched,
                           std::bind(bi::query24::Execute, std::placeholders::_1, tagClass, limit),
                           callback, std::bind([=](){
                             delete [] tagClass;
                             }));
  } break;
  case kBi25: {
    long long personA = std::stol(parameters["id1"][0]);
    long long personB = std::stol(parameters["id2"][0]);
    long long startDate = std::stol(parameters["startDate"][0]);
    long long endDate = std::stol(parameters["endDate"][0]);
    return std::make_tuple(kBi25, dispatched,
                           std::bind(bi::query25::Execute, std::placeholders::_1, personA, personB, startDate, endDate),
                           callback, std::bind([=](){
                             }));
  } break;
  default:
    return std::make_tuple(kNumQueries, dispatched, std::bind([]() {
      fprintf(stderr, "Unexpected query\n");
      return datatypes::Buffer(0,0);
    }), callback, std::bind([](){}));
    break;
  }
}
};

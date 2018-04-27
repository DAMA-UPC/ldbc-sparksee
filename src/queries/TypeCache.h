
#ifndef TYPECACHE_H
#define TYPECACHE_H
#include <gdb/common.h>
#include <boost/signals2/mutex.hpp>
#include <map>

#define SNBTYPEMACRO(varname, typestring) gdb::type_t varname;
#define SNBTYPELIST                                                            \
  SNBTYPEMACRO(comment_t, L"comment")                                          \
      SNBTYPEMACRO(email_address_t, L"emailaddress")                           \
      SNBTYPEMACRO(forum_t, L"forum")  \
      SNBTYPEMACRO(language_t, L"language")    \
      SNBTYPEMACRO(organization_t, L"organisation")                            \
      SNBTYPEMACRO(person_t, L"person") \
      SNBTYPEMACRO(place_t, L"place")       \
      SNBTYPEMACRO(post_t, L"post") \
      SNBTYPEMACRO(tag_t, L"tag")                \
      SNBTYPEMACRO(tagclass_t, L"tagclass")                                    \
      SNBTYPEMACRO(comment_has_creator_t, L"commentHasCreator")                \
      SNBTYPEMACRO(email_t, L"email")                                          \
      SNBTYPEMACRO(has_interest_t, L"hasInterest")                             \
      SNBTYPEMACRO(has_member_t, L"hasMember")                                 \
      SNBTYPEMACRO(has_member_with_posts_t, L"hasMemberWithPosts")             \
      SNBTYPEMACRO(has_moderator_t, L"hasModerator")                           \
      SNBTYPEMACRO(has_tag_t, L"hasTag") \
      SNBTYPEMACRO(has_type_t, L"hasType")  \
      SNBTYPEMACRO(is_located_in_t, L"isLocatedIn")                            \
      SNBTYPEMACRO(is_part_of_t, L"isPartOf")                                  \
      SNBTYPEMACRO(is_subclass_of_t, L"isSubclassOf")                          \
      SNBTYPEMACRO(knows_t, L"knows")  \
      SNBTYPEMACRO(likes_t, L"likes")          \
      SNBTYPEMACRO(container_of_t, L"containerOf")                  \
      SNBTYPEMACRO(post_has_creator_t, L"postHasCreator")                      \
      SNBTYPEMACRO(post_has_tag_t, L"postHasTag")                      \
      SNBTYPEMACRO(comment_has_tag_t, L"commentHasTag")                      \
      SNBTYPEMACRO(speaks_t, L"speaks") \
      SNBTYPEMACRO(study_at_t, L"studyAt")   \
      SNBTYPEMACRO(work_at_t, L"workAt") \
      SNBTYPEMACRO(reply_of_t, L"replyOf")

#define SNBTYPEATTRIBUTEMACRO(varname, typevarname, attribute)                 \
  gdb::attr_t varname;
#define SNBTYPEATTRIBUTELIST                                                   \
  SNBTYPEATTRIBUTEMACRO(person_id_t, person_t, L"id")                          \
      SNBTYPEATTRIBUTEMACRO(person_first_name_t, person_t, L"firstName")       \
      SNBTYPEATTRIBUTEMACRO(person_last_name_t, person_t, L"lastName")         \
      SNBTYPEATTRIBUTEMACRO(person_gender_t, person_t, L"gender")              \
      SNBTYPEATTRIBUTEMACRO(person_birthday_t, person_t, L"birthday")          \
      SNBTYPEATTRIBUTEMACRO(person_creation_date_t, person_t, L"creationDate") \
      SNBTYPEATTRIBUTEMACRO(person_locationIP_t, person_t, L"locationIP")      \
      SNBTYPEATTRIBUTEMACRO(person_browser_used_t, person_t, L"browserUsed")   \
      SNBTYPEATTRIBUTEMACRO(knows_creation_date_t, knows_t, L"creationDate")   \
      SNBTYPEATTRIBUTEMACRO(place_id_t, place_t, L"id")                        \
      SNBTYPEATTRIBUTEMACRO(place_name_t, place_t, L"name")                    \
      SNBTYPEATTRIBUTEMACRO(place_type_t, place_t, L"type")                    \
      SNBTYPEATTRIBUTEMACRO(organization_id_t, organization_t, L"id")          \
      SNBTYPEATTRIBUTEMACRO(organization_name_t, organization_t, L"name")      \
      SNBTYPEATTRIBUTEMACRO(organization_type_t, organization_t, L"type")      \
      SNBTYPEATTRIBUTEMACRO(study_at_classyear_t, study_at_t, L"classYear")    \
      SNBTYPEATTRIBUTEMACRO(work_at_work_from_t, work_at_t, L"workFrom")       \
      SNBTYPEATTRIBUTEMACRO(language_language_t, language_t, L"language")      \
      SNBTYPEATTRIBUTEMACRO(email_address_email_address_t, email_address_t,    \
                            L"emailaddress")                                   \
      SNBTYPEATTRIBUTEMACRO(tagclass_id_t, tagclass_t, L"id")                  \
      SNBTYPEATTRIBUTEMACRO(tagclass_name_t, tagclass_t, L"name")              \
      SNBTYPEATTRIBUTEMACRO(tag_id_t, tag_t, L"id")                        \
      SNBTYPEATTRIBUTEMACRO(tag_name_t, tag_t, L"name")                        \
      SNBTYPEATTRIBUTEMACRO(forum_id_t, forum_t, L"id")                        \
      SNBTYPEATTRIBUTEMACRO(forum_title_t, forum_t, L"title")                  \
      SNBTYPEATTRIBUTEMACRO(forum_creation_date_t, forum_t, L"creationDate")   \
      SNBTYPEATTRIBUTEMACRO(has_member_join_date_t, has_member_t, L"joinDate") \
      SNBTYPEATTRIBUTEMACRO(has_member_with_posts_join_date_t,                 \
                            has_member_with_posts_t, L"joinDate")              \
      SNBTYPEATTRIBUTEMACRO(post_id_t, post_t, L"id")                          \
      SNBTYPEATTRIBUTEMACRO(post_image_file_t, post_t, L"imageFile")           \
      SNBTYPEATTRIBUTEMACRO(post_creation_date_t, post_t, L"creationDate")     \
      SNBTYPEATTRIBUTEMACRO(post_locationIP_t, post_t, L"locationIP")          \
      SNBTYPEATTRIBUTEMACRO(post_browser_used_t, post_t, L"browserUsed")       \
      SNBTYPEATTRIBUTEMACRO(post_language_t, post_t, L"language")              \
      SNBTYPEATTRIBUTEMACRO(post_content_t, post_t, L"content")                \
      SNBTYPEATTRIBUTEMACRO(post_length_t, post_t, L"length")                  \
      SNBTYPEATTRIBUTEMACRO(likes_creation_date_t, likes_t, L"creationDate")   \
      SNBTYPEATTRIBUTEMACRO(comment_id_t, comment_t, L"id")                    \
      SNBTYPEATTRIBUTEMACRO(comment_creation_date_t, comment_t,                \
                            L"creationDate")                                   \
      SNBTYPEATTRIBUTEMACRO(comment_locationIP_t, comment_t, L"locationIP")    \
      SNBTYPEATTRIBUTEMACRO(comment_browser_used_t, comment_t, L"browserUsed") \
      SNBTYPEATTRIBUTEMACRO(comment_content_t, comment_t, L"content")          \
      SNBTYPEATTRIBUTEMACRO(comment_length_t, comment_t, L"length")

namespace sparksee {

namespace gdb {
class Graph;
}

namespace snb {
class TypeCache {
public:
  static TypeCache *instance(gdb::Graph *graph);
  SNBTYPELIST
  SNBTYPEATTRIBUTELIST
  gdb::attr_t knows_similarity_t;
  gdb::attr_t post_country_date_t;
  gdb::attr_t comment_country_date_t;
private:
  static TypeCache *instance_;
  TypeCache(gdb::Graph *);
  static boost::signals2::mutex mutex_;
};
}
}

#undef SNBTYPEMACRO
#undef SNBTYPEATTRIBUTEMACRO
#endif

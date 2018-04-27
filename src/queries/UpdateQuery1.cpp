#include "snbInteractive.h"
#include "Database.h"
#include "TypeCache.h"
#include "Utils.h"
#include <utils/Utils.h>

#include <gdb/Graph.h>
#include <gdb/Objects.h>
#include <gdb/ObjectsIterator.h>
#include <gdb/Session.h>
#include <gdb/Sparksee.h>
#include <gdb/Value.h>
#include <algorithm>
#include <stdio.h>
#include <boostPatch/property_tree/ptree.hpp>
#include <boostPatch/property_tree/json_parser.hpp>

namespace interactive {
  namespace update1{

    namespace gdb = sparksee::gdb;
    namespace snb = sparksee::snb;
    namespace ptree = boost::property_tree;

    datatypes::Buffer Execute(gdb::Session *sess, long long person_id, const char* first_name, const char* last_name,
        const char* gender, long long birthday, long long creation_date, const char* location_ip, const char* browser_used,
        long long city_id, int num_emails, const char* emails[], int num_languages, const char* languages[],
        int num_interests, const long long interests[], int num_work_ats, const long long work_ats[], 
        int num_study_ats, const long long study_ats[]) {
#ifdef VERBOSE
      printf("UPDATE QUERY1: ADD PERSON %llu %s\n", person_id, first_name);
#endif

        BEGIN_EXCEPTION
        BEGIN_UPDATE_TRANSACTION
        gdb::Graph *graph = sess->GetGraph();
        gdb::Value val;
        snb::TypeCache *cache = snb::TypeCache::instance(graph);
        gdb::oid_t person_oid = graph->NewNode(cache->person_t);
        val.SetLong(person_id);
        graph->SetAttribute(person_oid, cache->person_id_t, val);

        val.SetString(sparksee::utils::to_wstring(location_ip));
        graph->SetAttribute(person_oid, cache->person_locationIP_t, val);

        val.SetString(sparksee::utils::to_wstring(first_name));
        graph->SetAttribute(person_oid, cache->person_first_name_t, val);

        val.SetString(sparksee::utils::to_wstring(last_name));
        graph->SetAttribute(person_oid, cache->person_last_name_t, val);

        val.SetString(sparksee::utils::to_wstring(gender));
        graph->SetAttribute(person_oid, cache->person_gender_t, val);

        std::string birthday_str = sparksee::utils::format_date(birthday);
        val.SetString(sparksee::utils::to_wstring(birthday_str));
        graph->SetAttribute(person_oid, cache->person_birthday_t, val);

        val.SetTimestamp(creation_date);
        graph->SetAttribute(person_oid, cache->person_creation_date_t, val);

        val.SetString(sparksee::utils::to_wstring(browser_used));
        graph->SetAttribute(person_oid, cache->person_browser_used_t, val);
        val.SetLong(city_id);
        gdb::oid_t city_oid = graph->FindObject(cache->place_id_t, val);
        graph->NewEdge(cache->is_located_in_t, person_oid, city_oid);

        for(int i = 0; i < num_languages; ++i) {
          val.SetString(sparksee::utils::to_wstring(languages[i]));
          gdb::oid_t language_oid = graph->FindObject(cache->language_language_t, val);
	  if (language_oid == gdb::Objects::InvalidOID) {
		  language_oid = graph->NewNode(cache->language_t);
		  graph->SetAttribute(language_oid, cache->language_language_t, val);
	  }
          graph->NewEdge(cache->speaks_t, person_oid, language_oid); 
        }

        for(int i = 0; i < num_emails; ++i) {
          val.SetString(sparksee::utils::to_wstring(emails[i]));
          gdb::oid_t email_oid = graph->NewNode(cache->email_address_t);
          graph->SetAttribute(email_oid, cache->email_address_email_address_t, val);
          graph->NewEdge(cache->email_t, person_oid, email_oid); 
        }

        for(int i = 0; i < num_interests; ++i) {
          long long interest_id = interests[i];
          val.SetLong(interest_id);
          gdb::oid_t interest_oid = graph->FindObject(cache->tag_id_t, val);
          graph->NewEdge(cache->has_interest_t, person_oid, interest_oid);
        }


        for( int i = 0; i < num_study_ats; ++i) {
          long long university_id = study_ats[i*2];
          int year = study_ats[i*2+1];
          val.SetLong(university_id);
          gdb::oid_t university_oid = graph->FindObject(cache->organization_id_t, val);
          gdb::oid_t study_at_oid = graph->NewEdge(cache->study_at_t, person_oid, university_oid);
          val.SetInteger(year);
          graph->SetAttribute(study_at_oid,cache->study_at_classyear_t, val);
        }

        for( int i = 0; i < num_work_ats; ++i) {
          long long company_id = work_ats[i*2];
          int year = work_ats[i*2+1];
          val.SetLong(company_id);
          gdb::oid_t company_id_oid = graph->FindObject(cache->organization_id_t, val);
          gdb::oid_t work_at_oid = graph->NewEdge(cache->work_at_t, person_oid, company_id_oid);
          val.SetInteger(year);
          graph->SetAttribute(work_at_oid,cache->work_at_work_from_t, val);
        }
        COMMIT_TRANSACTION
        delete graph;
#ifdef VERBOSE
      printf("EXIT UPDATE QUERY1: ADD PERSON %llu %s\n", person_id, first_name);
#endif

      char* res = snb::utils::empty_json();
      return datatypes::Buffer(res,strlen(res));
      END_EXCEPTION
    }
  }
}

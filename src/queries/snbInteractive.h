#ifndef _SNB_INTERACTIVE_H_
#define _SNB_INTERACTIVE_H_

#include "../server/utils/DataTypes.h"
#include <gdb/Session.h>

namespace gdb = sparksee::gdb;

namespace interactive {

namespace query1 {
datatypes::Buffer Execute(gdb::Session *session, long long person_id,
                    const char *first_name, unsigned int limit);
}

namespace query2 {
datatypes::Buffer Execute(gdb::Session *session, long long person_id,
                    long long max_date, unsigned int limit);
}

namespace query3 {
datatypes::Buffer Execute(gdb::Session *session, long long person_id,
                    const char *country1, const char *country2, long long date,
                    unsigned int duration, unsigned int limit);
}

namespace query4 {
datatypes::Buffer Execute(gdb::Session *session, long long person_id,
                    long long start_date, unsigned int days,
                    unsigned int limit);
}

namespace query5 {
datatypes::Buffer Execute(gdb::Session *session, long long person_id,
                    long long min_date, unsigned int limit);
}

namespace query6 {
datatypes::Buffer Execute(gdb::Session *session, long long person_id,
                    const char *tag, unsigned int limit);
}

namespace query7 {
datatypes::Buffer Execute(gdb::Session *session, long long person_id,
                    unsigned int limit);
}

namespace query8 {
datatypes::Buffer Execute(gdb::Session *session, long long person_id,
                    unsigned int limit);
}

namespace query9 {
datatypes::Buffer Execute(gdb::Session *session, long long person_id,
                    long long max_date, unsigned int limit);
}

namespace query10 {
datatypes::Buffer Execute(gdb::Session *session, long long person_id,
                    unsigned int month, unsigned int limit);
}

namespace query11 {
datatypes::Buffer Execute(gdb::Session *session, long long person_id,
                    const char *country, int year, unsigned int limit);
}

namespace query12 {
datatypes::Buffer Execute(gdb::Session *session, long long person_id,
                    const char *tag_class, unsigned int limit);
}

namespace query13 {
datatypes::Buffer Execute(gdb::Session *session, long long person1,
                    long long person2);
}

namespace query14 {
datatypes::Buffer Execute(gdb::Session *session, long long person1,
                    long long person2);
}

namespace update1 {
datatypes::Buffer Execute(gdb::Session *session, long long person_id,
                    const char *first_name, const char *last_name,
                    const char *gender, long long birthday,
                    long long creation_date, const char *location_ip,
                    const char *browser_used, long long city_id, int num_emails,
                    const char *emails[], int num_languages,
                    const char *languages[], int num_interests,
                    const long long interests[], int num_work_ats,
                    const long long work_ats[], int num_study_ats,
                    const long long study_ats[]);
}

namespace update2 {
datatypes::Buffer Execute(gdb::Session *session, long long person_id,
                    long long post_id, long long creation_date);
}

namespace update3 {
datatypes::Buffer Execute(gdb::Session *session, long long person_id,
                    long long comment_id, long long creation_date);
}

namespace update4 {
datatypes::Buffer Execute(gdb::Session *session, long long forum_id,
                    const char *title, long long creation_date,
                    long long moderator_id, int num_tags, long long tags[]);
}

namespace update5 {
datatypes::Buffer Execute(gdb::Session *session, long long person_id,
                    long long forum_id, long long join_date);
}

namespace update6 {
datatypes::Buffer Execute(gdb::Session *session, long long post_id,
                    const char *image_file, long long creation_date,
                    const char *location_ip, const char *browser_used,
                    const char *language, const char *content, int length,
                    long long creator_id, long long forum_id,
                    long long location_id, int num_tags,
                    const long long tags[]);
}

namespace update7 {
datatypes::Buffer Execute(gdb::Session *session, long long comment_id,
                    long long creation_date, const char *location_ip,
                    const char *browser_used, const char *content, int length,
                    long long creator_id, long long location_id,
                    long long reply_of_post_id, long long reply_of_comment_id,
                    int num_tags, const long long tags[]);
}

namespace update8 {
datatypes::Buffer Execute(gdb::Session *session, long long person1_id,
                    long long person2_id, long long creation_date);
}

namespace short1 {
datatypes::Buffer Execute(gdb::Session *session, long long person);
}

namespace short2 {
datatypes::Buffer Execute(gdb::Session *session, long long person);
}

namespace short3 {
datatypes::Buffer Execute(gdb::Session *session, long long person);
}

namespace short4 {
datatypes::Buffer Execute(gdb::Session *session, long long message);
}

namespace short5 {
datatypes::Buffer Execute(gdb::Session *session, long long message);
}

namespace short6 {
datatypes::Buffer Execute(gdb::Session *session, long long message);
}

namespace short7 {
datatypes::Buffer Execute(gdb::Session *session, long long message);
}

}
#endif // _SNB_INTERACTIVE_H_

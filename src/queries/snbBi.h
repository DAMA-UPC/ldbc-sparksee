
#ifndef _SNB_BI_H_
#define _SNB_BI_H_

#include "../server/utils/DataTypes.h"


namespace sparksee {
class Database;
namespace gdb {
class Session;
}
}

namespace gdb = sparksee::gdb;

namespace bi {

namespace query1 {
 datatypes::Buffer Execute(gdb::Session* sess, long long date);
}

namespace query2 {
 datatypes::Buffer Execute(gdb::Session* sess, long long date1, long long date2, const char* country1, const char* country2, unsigned int limit);
}

namespace query3 {
 datatypes::Buffer Execute(gdb::Session* sess, unsigned int year, unsigned int month, unsigned int limit);
}

namespace query4 {
 datatypes::Buffer Execute(gdb::Session* sess, const char* tag_class, const char* country, unsigned int limit);
}
namespace query5 {
 datatypes::Buffer Execute(gdb::Session* sess, const char* country, unsigned int limit);
}
namespace query6 {
 datatypes::Buffer Execute(gdb::Session* sess, const char* tag, unsigned int limit);
}
namespace query7 {
 datatypes::Buffer Execute(gdb::Session* sess, const char* tag, unsigned int limit);
}
namespace query8 {
 datatypes::Buffer Execute(gdb::Session* sess, const char* tag, unsigned int limit);
}
namespace query9 {
 datatypes::Buffer Execute(gdb::Session* sess, const char* tag_class1, const char* tag_class2, int threshold, int limit);
}
namespace query10 {
 datatypes::Buffer Execute(gdb::Session* sess, const char* tag, long long date, unsigned int limit);
}
namespace query11 {
 datatypes::Buffer Execute(gdb::Session* sess, const char* country, const char** blacklist, int blacklist_size, int limit );
}
namespace query12 {
 datatypes::Buffer Execute(gdb::Session* sess, long long creation_date, int like_count, int limit);
}
namespace query13 {
 datatypes::Buffer Execute(gdb::Session* sess, const char* country, int limit);
}
namespace query14 {
 datatypes::Buffer Execute(gdb::Session* sess, long long date_begin, long long date_end, int limit);
}
namespace query15 {
 datatypes::Buffer Execute(gdb::Session* sess, const char* country, int limit);
}
namespace query16 {
 datatypes::Buffer Execute(gdb::Session* sess, long long person_id, const char* country_name, const char* tag_class_name, int min, int max, int limit);
}
namespace query17 {
 datatypes::Buffer Execute(gdb::Session* sess, const char* country_name, int limit);
}
namespace query18 {
 datatypes::Buffer Execute(gdb::Session* sess, long long creation_date, int num_languages, const char** languages, unsigned int threshold, int limit);
}
namespace query19 {
 datatypes::Buffer Execute(gdb::Session* sess, long long date, const char* tag_class1, const char* tag_class2, int limit);
}
namespace query20 {
 datatypes::Buffer Execute(gdb::Session* sess, const char** tagClasses, int numTagClasses, int limit);
}
namespace query21 {
 datatypes::Buffer Execute(gdb::Session* sess, const char* country_name, long long end_date, int limit);
}
namespace query22 {
 datatypes::Buffer Execute(gdb::Session* sess, const char* country1, const char* country2, int limit);
}
namespace query23 {
 datatypes::Buffer Execute(gdb::Session* sess, const char* country_name, int limit);
}
namespace query24 {
 datatypes::Buffer Execute(gdb::Session* sess, const char* tag_class_name, int limit);
}

namespace query25 {
 datatypes::Buffer Execute(gdb::Session *sess, long long person1, long long person2, long long startDate, long long endDate);
}

}
#endif // _SNB_BI_H_

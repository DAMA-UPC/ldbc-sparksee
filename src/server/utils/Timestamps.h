#ifndef _UTILS_TIMESTAMPS_H_
#define _UTILS_TIMESTAMPS_H_

#include <string>

namespace timestamp {

unsigned long string_to_timestamp(std::string date);

unsigned long current();

unsigned long current_micros();
}

#endif /* _UTILS_TIMESTAMPS_H_ */

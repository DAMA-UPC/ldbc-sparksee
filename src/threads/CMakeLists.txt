#
#SNB Queries
#

if (CMAKE_COMPILER_IS_GNUCC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -std=c++11 -Wl,--no-as-needed")
SET(CMAKE_CXX_FLAGS_DEBUG "-g -pg ")
SET(CMAKE_CXX_FLAGS_RELEASE "-O3")
endif(CMAKE_COMPILER_IS_GNUCC)

add_definitions(${DEFAULT_DEFINES})
include_directories(${DEFAULT_INCLUDES})
include_directories(${SPARKSEE_INCLUDES})
include_directories(${BOOST_PATCH_INCLUDES})
include_directories(${Boost_INCLUDE_DIR})
include_directories(${UTF8_INCLUDES})

add_library(  threads STATIC 
		Threads.cpp 
)

#target_link_libraries(threads 
#${Boost_LIBRARIES})

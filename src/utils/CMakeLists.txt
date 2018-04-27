#
#SNB Queries
#

if (CMAKE_COMPILER_IS_GNUCC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wl,--no-as-needed -DCAPTURE_EXCEPTIONS")
SET(CMAKE_CXX_FLAGS_DEBUG "-g -pg ")
SET(CMAKE_CXX_FLAGS_RELEASE "-O3")
endif(CMAKE_COMPILER_IS_GNUCC)

add_definitions(${DEFAULT_DEFINES})
include_directories(${DEFAULT_INCLUDES})
include_directories(${SPARKSEE_INCLUDES})
include_directories(${BOOST_PATCH_INCLUDES})
include_directories(${Boost_INCLUDE_DIR})
include_directories(${UTF8_INCLUDES})

add_library(  sparkseeutils STATIC 
		Utils.cpp 
    		GroupBy.cpp
)

target_link_libraries(sparkseeutils 
${SPARKSEE_LIB}
${Boost_LIBRARIES})

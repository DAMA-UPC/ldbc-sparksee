#
# SNB LOADER
#
add_definitions(${DEFAULT_DEFINES})
include_directories(${DEFAULT_INCLUDES})
include_directories(${SPARKSEE_INCLUDES})
INCLUDE_DIRECTORIES(${BOOST_PATCH_INCLUDES})
include_directories(${Boost_INCLUDE_DIR})

set(CMAKE_CXX_FLAGS "-pthread -std=c++11")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -pg")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

add_library(snbLoader STATIC
  snbLoader.cpp
)

target_link_libraries(snbLoader
  ${SPARKSEE_LIB}
  snbInteractiveQueries 
)

add_executable(snbLoaderStandalone
  snbLoaderStandalone.cpp
)

add_executable(script_loader
	script_loader.cpp
	)

target_link_libraries(script_loader
	${SPARKSEE_LIB}
  	${PTHREADS_LIB}
)

add_executable(precompute
	precompute.cpp
)

add_executable(simple_dump
	simple_dump.cpp
)

target_link_libraries(precompute
	${SPARKSEE_LIB}
	sparkseeutils
  snbInteractiveQueries 
  threads
  ${PTHREADS_LIB}
)

target_link_libraries(snbLoaderStandalone
    snbLoader 
    sparkseeutils
)

target_link_libraries(simple_dump
	${SPARKSEE_LIB}
	sparkseeutils
  snbInteractiveQueries 
  threads
  ${PTHREADS_LIB}
)


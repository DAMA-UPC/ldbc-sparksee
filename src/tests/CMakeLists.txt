#
# TESTS
#
add_definitions(${DEFAULT_DEFINES})
include_directories(${DEFAULT_INCLUDES})
include_directories(${SPARKSEE_INCLUDES})
include_directories(${GTEST_INCLUDES})
include_directories(${BOOST_PATCH_INCLUDES})
INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -std=c++11")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Weverything -Wno-unused-variable -Wno-c++98-compat -Wno-padded -Wno-weak-vtables -Wno-unreachable-code-break -Wno-unreachable-code-loop-increment -Wno-c++98-compat-pedantic")
if (CMAKE_COMPILER_IS_GNUCC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wl,--no-as-needed")
set(CMAKE_CXX_FLAGS_DEBUG "-g -pg ")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
endif(CMAKE_COMPILER_IS_GNUCC)
add_definitions(${CMAKE_CXX_FLAGS})

add_executable(LoaderTest
  LoaderTest.cpp
)

add_executable(LoadPerformanceTest
  LoadPerformanceTest.cpp
)

add_executable(IntersectionPerformanceTest
  IntersectionPerformanceTest.cpp
)

add_executable(MultithreadedUpdatesTest
  MultithreadedUpdatesTest.cpp
)

add_executable(MultithreadedReadsTest
  MultithreadedReadsTest.cpp
)

target_link_libraries(LoaderTest
  snbLoader
  gtest
  threads
  ${SPARKSEE_LIB}
  ${PTHREADS_LIB}
)

target_link_libraries(LoadPerformanceTest
  threads
  ${SPARKSEE_LIB}
  ${PTHREADS_LIB}
)

target_link_libraries(IntersectionPerformanceTest
  threads
  ${SPARKSEE_LIB}
  ${PTHREADS_LIB}
)

target_link_libraries(MultithreadedUpdatesTest
  sparkseeutils
  threads
  ${SPARKSEE_LIB}
  ${PTHREADS_LIB}
  ${Boost_LIBRARIES}
)

target_link_libraries(MultithreadedReadsTest
  threads
  sparkseeutils
  ${SPARKSEE_LIB}
  ${PTHREADS_LIB}
  ${Boost_LIBRARIES}
)

#
#SNB LOADER
#
add_definitions(${DEFAULT_DEFINES})
include_directories(${DEFAULT_INCLUDES})
include_directories(${SPARKSEE_INCLUDES})
INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})

INCLUDE_DIRECTORIES(${BOOST_PATCH_INCLUDES})
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -std=c++11")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Weverything -Wno-unused-variable -Wno-c++98-compat -Wno-padded -Wno-weak-vtables -Wno-unreachable-code-break -Wno-unreachable-code-loop-increment -Wno-c++98-compat-pedantic")
if (CMAKE_COMPILER_IS_GNUCC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wl,--no-as-needed")
set(CMAKE_CXX_FLAGS_DEBUG "-g -pg ")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
endif(CMAKE_COMPILER_IS_GNUCC)
add_definitions(${CMAKE_CXX_FLAGS})

add_executable(server 
		utils/Timestamps.cpp 
		Worker.cpp 
		QueryStatistics.cpp
		QueryStrategy.cpp 
		ThreadStrategy.cpp
		QueryStrategy/RoundRobin.cpp
		QueryStrategy/Remote.cpp
		ThreadStrategy/RoundRobin.cpp
    ThreadStrategy/ShortestJobFirst.cpp
    ThreadStrategy/UpdateBatching.cpp
    ThreadStrategy/DoubleBuffer.cpp
    ThreadStrategy/DoubleBufferEncore.cpp
		Server.cpp)

  target_link_libraries(server ${SPARKSEE_LIB} snbInteractiveQueries
    ${PTHREADS_LIB} ${Boost_LIBRARIES} threads )

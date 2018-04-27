#
#SNB Queries
#

if (CMAKE_COMPILER_IS_GNUCC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wl,--no-as-needed -DCAPTURE_EXCEPTIONS -std=c++03")
SET(CMAKE_CXX_FLAGS_DEBUG "-g -pg -std=c++03")
SET(CMAKE_CXX_FLAGS_RELEASE "-O3 -std=c++03")
endif(CMAKE_COMPILER_IS_GNUCC)

add_definitions(${DEFAULT_DEFINES})
include_directories(${DEFAULT_INCLUDES})
include_directories(${SPARKSEE_INCLUDES})
include_directories(${BOOST_PATCH_INCLUDES})
INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})
include_directories(${UTF8_INCLUDES})

add_library(  snbInteractiveQueries STATIC 
		Database.cpp 
		TypeCache.cpp 
    		Utils.cpp
		InteractiveQuery1.cpp 
		InteractiveQuery2.cpp 
		InteractiveQuery3.cpp 
		InteractiveQuery4.cpp 
		InteractiveQuery5.cpp 
		InteractiveQuery6.cpp
		InteractiveQuery7.cpp
		InteractiveQuery8.cpp
		InteractiveQuery9.cpp
		InteractiveQuery10.cpp
		InteractiveQuery11.cpp
		InteractiveQuery12.cpp
		InteractiveQuery13.cpp
		InteractiveQuery14.cpp
        UpdateQuery1.cpp
        UpdateQuery2.cpp
        UpdateQuery3.cpp
        UpdateQuery4.cpp
        UpdateQuery5.cpp
        UpdateQuery6.cpp
        UpdateQuery7.cpp
        UpdateQuery8.cpp
        ShortQuery1.cpp
        ShortQuery2.cpp
        ShortQuery3.cpp
        ShortQuery4.cpp
        ShortQuery5.cpp
        ShortQuery6.cpp
        ShortQuery7.cpp
        BiQuery1.cpp
        BiQuery2.cpp
        BiQuery3.cpp
        BiQuery4.cpp
        BiQuery5.cpp
        BiQuery6.cpp
        BiQuery7.cpp
        BiQuery8.cpp
        BiQuery9.cpp
        BiQuery10.cpp
        BiQuery11.cpp
        BiQuery12.cpp
        BiQuery13.cpp
        BiQuery14.cpp
        BiQuery15.cpp
        BiQuery16.cpp
        BiQuery17.cpp
        BiQuery18.cpp
        BiQuery19.cpp
        BiQuery20.cpp
        BiQuery21.cpp
        BiQuery22.cpp
        BiQuery23.cpp
        BiQuery24.cpp
        BiQuery25.cpp
        )

target_link_libraries(snbInteractiveQueries sparkseeutils
${SPARKSEE_LIB}
${Boost_LIBRARIES}
)

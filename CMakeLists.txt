cmake_minimum_required(VERSION 2.6 FATAL_ERROR)
project(LDBCpp)

set( CMAKE_EXPORT_COMPILE_COMMANDS 1 )

include(CTest) # force to build tests

# Check 64bits architecture
if(CMAKE_SIZEOF_VOID_P MATCHES "4")
  set(ARCHTYPE "32")
else(CMAKE_SIZEOF_VOID_P MATCHES "4")
  set(ARCHTYPE "64")
endif(CMAKE_SIZEOF_VOID_P MATCHES "4")

set(DEFAULT_DEFINES "-fno-omit-frame-pointer -ggdb -g -fPIC
-D_FILE_OFFSET_BITS=64 -Wall -Werror -DBOOST_SPIRIT_THREADSAFE
-Wno-error=deprecated -DCAPTURE_EXCEPTIONS")

set(Boost_USE_STATIC_LIBS OFF) 
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 
FIND_PACKAGE(Boost COMPONENTS program_options date_time thread system locale REQUIRED)

if(WIN32)
  set(WINDOWS 1)
  set(SO "windows")
elseif(UNIX)
  if(APPLE)
    set(MACOS 1)
    set(SO "macos")
  else(APPLE)
    set(LINUX 1)
    set(SO "linux")
  endif(APPLE)
else(WIN32)
  set(SO "OTHER")
endif(WIN32)

# INCLUDE DEFINITIONS
set(DEFAULT_INCLUDES "${LDBCpp_SOURCE_DIR}/src")
# so sparksee includes can be found in its own .h i do not like having gdb/Class.h instead of sparksee/gdb/Class.h.
#But i will not change it now in its source files.
set(GTEST_INCLUDES    "${LDBCpp_SOURCE_DIR}/libs/gtest-1.7.0/include/")
set(BOOST_PATCH_INCLUDES "${LDBCpp_SOURCE_DIR}/libs/boostPatch/")
set(UTF8_INCLUDES "${LDBCpp_SOURCE_DIR}/libs/utf8/")

if (NOT DEFINED ENV{SPARKSEE_ROOT})
  message(FATAL_ERROR "SPARKSEE_ROOT variable not defined")
endif(NOT DEFINED ENV{SPARKSEE_ROOT})

set(SPARKSEE_INCLUDES  "$ENV{SPARKSEE_ROOT}/includes/sparksee")
set(SPARKSEE_LIB_DIR "$ENV{SPARKSEE_ROOT}/lib/linux64")

MESSAGE(STATUS "SPARKSEE_INCLUDES: ${SPARKSEE_INCLUDES}")
MESSAGE(STATUS "SPARKSEE_LIB_DIR: ${SPARKSEE_LIB_DIR}")

# LIBRARY DEFINITIONS
if(WINDOWS)
  set(PTHREADS_LIB "")
  if(ARCHTYPE EQUAL 32)
    #set(SPARKSEE_LIB "${SPARKSEE_LIB}/windows32_VS2013/sparksee.dll")
    set(SPARKSEE_LIB "${SPARKSEE_LIB_DIR}/sparksee.dll")
  else(ARCHTYPE EQUAL 32)
    #set(SPARKSEE_LIB "${SPARKSEE_LIB}/windows54_VS2013/sparksee.dll")
    set(SPARKSEE_LIB "${SPARKSEE_LIB_DIR}/sparksee.dll")
  endif(ARCHTYPE EQUAL 32)
else(WINDOWS)
  set(PTHREADS_LIB "pthread")
  if(APPLE)
    #set(SPARKSEE_LIB "${SPARKSEE_LIB}/macos64/libsparksee.dylib")
    set(SPARKSEE_LIB "${SPARKSEE_LIB_DIR}/libsparksee.dylib")
  else(APPLE)
    if(ARCHTYPE EQUAL 32)
      #set(SPARKSEE_LIB "${SPARKSEE_LIB}/linux32/libsparksee.so")
      set(SPARKSEE_LIB "${SPARKSEE_LIB_DIR}libsparksee.so")
    else(ARCHTYPE EQUAL 32)
      #set(SPARKSEE_LIB "${SPARKSEE_LIB}/linux64/libsparksee.so")
      set(SPARKSEE_LIB "${SPARKSEE_LIB_DIR}/libsparksee.so")
    endif(ARCHTYPE EQUAL 32)
  endif(APPLE)
endif(WINDOWS)

MESSAGE(STATUS "SPARKSEE_LIB: ${SPARKSEE_LIB}")

# generate static libraries into the root build directory
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${LDBCpp_BINARY_DIR})
# generate shared libraries into the root build directory
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${LDBCpp_BINARY_DIR})
# generate execs into the root build directory
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${LDBCpp_BINARY_DIR})
# link with generated libraries into the root build directory 
link_directories(${LDBCpp_BINARY_DIR})

add_subdirectory("${LDBCpp_SOURCE_DIR}/libs/gtest-1.7.0")
add_subdirectory("${LDBCpp_SOURCE_DIR}/src/utils")
add_subdirectory("${LDBCpp_SOURCE_DIR}/src/threads")
add_subdirectory("${LDBCpp_SOURCE_DIR}/src/queries")
add_subdirectory("${LDBCpp_SOURCE_DIR}/src/server")
add_subdirectory("${LDBCpp_SOURCE_DIR}/src/tools")
add_subdirectory("${LDBCpp_SOURCE_DIR}/src/tests")

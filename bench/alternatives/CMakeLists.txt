# alt_bench: fastipc against other IPC libraries (README.md). The libraries come from deps.py's prefix
# (build/deps/<os>/prefix, or -DALT_DEPS_PREFIX=...), fastipc from this repository's zig-out (zig build
# -Doptimize=ReleaseFast). A library that isn't found is left out.
cmake_minimum_required(VERSION 3.22)
project(alt_bench CXX C)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release)
endif()

if(WIN32)
  set(alt_os windows)
elseif(APPLE)
  set(alt_os macos)
else()
  set(alt_os linux)
endif()
set(ALT_DEPS_PREFIX "${CMAKE_CURRENT_SOURCE_DIR}/build/deps/${alt_os}/prefix" CACHE PATH "deps.py's prefix")
set(FASTIPC_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../.." CACHE PATH "the fastipc checkout")
list(PREPEND CMAKE_PREFIX_PATH "${ALT_DEPS_PREFIX}")

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_executable(alt_bench src/main.cpp)
target_include_directories(alt_bench PRIVATE src "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(alt_bench PRIVATE Threads::Threads)
if(MSVC)
  target_compile_options(alt_bench PRIVATE /O2 /W3 /EHsc /permissive- /Zc:__cplusplus)
  target_compile_definitions(alt_bench PRIVATE _CRT_SECURE_NO_WARNINGS BOOST_ALL_NO_LIB)
else()
  target_compile_options(alt_bench PRIVATE -O2)
endif()

set(alt_libs "")
macro(alt_add name source)
  target_sources(alt_bench PRIVATE "src/${source}")
  string(APPEND alt_libs "ALT_LIB(${name})\n")
  message(STATUS "alt_bench: ${name}")
endmacro()

# fastipc: the shared library of this checkout.
find_library(FASTIPC_LIBRARY NAMES fastipc PATHS "${FASTIPC_ROOT}/zig-out/lib" NO_DEFAULT_PATH REQUIRED)
target_include_directories(alt_bench PRIVATE "${FASTIPC_ROOT}/include")
target_link_libraries(alt_bench PRIVATE "${FASTIPC_LIBRARY}")
if(WIN32)
  target_compile_definitions(alt_bench PRIVATE FASTIPC_SHARED)
else()
  set_target_properties(alt_bench PROPERTIES BUILD_RPATH "${FASTIPC_ROOT}/zig-out/lib")
endif()
alt_add(lib_fastipc lib_fastipc.cpp)
string(APPEND alt_libs "ALT_LIB(lib_fastipc_zerocopy)\n")

alt_add(lib_uds lib_sockets.cpp)
string(APPEND alt_libs "ALT_LIB(lib_tcp)\n")
if(WIN32)
  target_link_libraries(alt_bench PRIVATE ws2_32)
endif()

find_package(ZeroMQ CONFIG QUIET)
if(TARGET libzmq-static)
  target_link_libraries(alt_bench PRIVATE libzmq-static)
  alt_add(lib_zmq lib_zmq.cpp)
  string(APPEND alt_libs "ALT_LIB(lib_zmq_tcp)\n")
endif()

find_package(nng CONFIG QUIET)
if(TARGET nng::nng)
  target_link_libraries(alt_bench PRIVATE nng::nng)
  alt_add(lib_nng lib_nng.cpp)
  string(APPEND alt_libs "ALT_LIB(lib_nng_tcp)\n")
endif()

find_package(cpp-ipc CONFIG QUIET)
if(TARGET cpp-ipc::ipc)
  target_link_libraries(alt_bench PRIVATE cpp-ipc::ipc)
  alt_add(lib_cppipc lib_cppipc.cpp)
endif()

if(EXISTS "${ALT_DEPS_PREFIX}/include/boost/interprocess/ipc/message_queue.hpp")
  target_include_directories(alt_bench SYSTEM PRIVATE "${ALT_DEPS_PREFIX}/include")
  if(UNIX AND NOT APPLE)
    target_link_libraries(alt_bench PRIVATE rt)
  endif()
  alt_add(lib_boost lib_boost.cpp)
endif()

find_package(iceoryx2-cxx CONFIG QUIET)
if(TARGET iceoryx2-cxx::static-lib-cxx)
  target_link_libraries(alt_bench PRIVATE iceoryx2-cxx::static-lib-cxx)
  if(APPLE)
    # Apple's linker (ld-prime) fails on iceoryx2's logger, a thread-local with a destructor in its headers ("invalid
    # use of ADRP/imm12 ... to ___dso_handle"); the classic linker links it.
    target_link_options(alt_bench PRIVATE -Wl,-ld_classic)
  endif()
  alt_add(lib_iceoryx2 lib_iceoryx2.cpp)
endif()

find_package(aeron CONFIG QUIET)
if(TARGET aeron::aeron_static)
  target_link_libraries(alt_bench PRIVATE aeron::aeron_static)
  target_include_directories(alt_bench PRIVATE "${ALT_DEPS_PREFIX}/include/aeron")
  alt_add(lib_aeron lib_aeron.cpp)
endif()

file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/libs.inc.new" "${alt_libs}")
file(COPY_FILE "${CMAKE_CURRENT_BINARY_DIR}/libs.inc.new" "${CMAKE_CURRENT_BINARY_DIR}/libs.inc" ONLY_IF_DIFFERENT)

# Next to the executable: the shared libraries it loads (Windows has no rpath).
if(WIN32)
  add_custom_command(TARGET alt_bench POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different "${FASTIPC_ROOT}/zig-out/bin/fastipc.dll" "$<TARGET_FILE_DIR:alt_bench>")
endif()

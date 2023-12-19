
project(libplasma)
find_package(Threads)
set(PLASMA_SRCS
        client.cc
        common.cc
        fling.cc
        io.cc
        malloc.cc
        plasma.cc
        protocol.cc)
set(PLASMA_STORE_SRCS
        dlmalloc.cc
        events.cc
        eviction_policy.cc
        quota_aware_policy.cc
        plasma_allocator.cc
        store.cc
        thirdparty/ae/ae.c)


add_library(plasma STATIC ${PLASMA_SRCS})
set_target_properties(plasma PROPERTIES ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/libs)

# The optimization flag -O3 is suggested by dlmalloc.c, which is #included in
# malloc.cc; we set it here regardless of whether we do a debug or release build.

set_source_files_properties(dlmalloc.cc PROPERTIES COMPILE_FLAGS "-O3")


if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set_property(SOURCE dlmalloc.cc
            APPEND_STRING
            PROPERTY COMPILE_FLAGS " -Wno-conversion")
endif ()

list(APPEND PLASMA_EXTERNAL_STORE_SOURCES "external_store.cc" "hash_table_store.cc")

link_directories(${CMAKE_CURRENT_BINARY_DIR})
# We use static libraries for the plasma-store-server executable so that it can
# be copied around and used in different locations.
add_executable(plasma-store-server ${PLASMA_EXTERNAL_STORE_SOURCES} ${PLASMA_STORE_SRCS})


target_link_libraries(plasma-store-server plasma
        "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>"
        gflags::gflags pthread)

set(PLASMA_STORE_SERVER_DIR ${CMAKE_SOURCE_DIR}/tools)
file(MAKE_DIRECTORY ${PLASMA_STORE_SERVER_DIR})
set_target_properties(plasma-store-server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${PLASMA_STORE_SERVER_DIR})
target_compile_definitions(plasma-store-server PRIVATE _PLASMA_NO_DEPRECATE)

if (ARROW_RPATH_ORIGIN)
    set(_lib_install_rpath "\$ORIGIN")
endif ()


set(PLASMA_HEADER_DIR ${CMAKE_SOURCE_DIR}/includes/plasma)
file(MAKE_DIRECTORY ${PLASMA_HEADER_DIR})
file(COPY client.h DESTINATION ${PLASMA_HEADER_DIR})
file(COPY common.h DESTINATION ${PLASMA_HEADER_DIR})
file(COPY compat.h DESTINATION ${PLASMA_HEADER_DIR})
file(COPY events.h DESTINATION ${PLASMA_HEADER_DIR})
file(COPY test_util.h DESTINATION ${PLASMA_HEADER_DIR})


add_dependencies(plasma-store-server plasma)








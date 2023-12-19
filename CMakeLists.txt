cmake_minimum_required(VERSION 3.5)
SET(CMAKE_TOOLCHAIN_FILE "~/Downloads/vcpkg/scripts/buildsystems/vcpkg.cmake")
project(pyplasma)


message("cmake system version:${CMAKE_SYSTEM}")
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")
message("cmake cxx compile version:${CMAKE_CXX_COMPILER_VERSION}")


set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-conversion")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -std=c++17")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -Wall")

execute_process(COMMAND ${PYTHON_EXECUTABLE} "-c" "print('hello,python!')")

set(Python3_FIND_STRATEGY "LOCATION")
set(Python3_FIND_REGISTRY "LAST")
set(Python3_FIND_FRAMEWORK "LAST")


if (CMAKE_VERSION VERSION_LESS 3.18.0)
    #development.module in cmake < 3.18 is not present
    find_package(Python3 COMPONENTS Interpreter Development NumPy)
else ()
    find_package(Python3 COMPONENTS Interpreter Development.Module NumPy)
endif ()


find_package(Arrow CONFIG REQUIRED)
find_package(gflags CONFIG REQUIRED)
find_package(pybind11 CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)

message("Python3_FOUND:${Python3_FOUND}")
message("Python3_VERSION:${Python3_VERSION}")
message("Python3_Development_FOUND:${Python3_Development_FOUND}")
message("Python3_SITE_LIB:" ${Python3_SITELIB})


include_directories(${CMAKE_SOURCE_DIR}/includes)

include_directories(${Python3_INCLUDE_DIRS})

#link pyarrow headers and libs
include_directories("${Python3_SITELIB}/pyarrow/include")
link_directories("${Python3_SITELIB}/pyarrow/")


add_subdirectory(${CMAKE_SOURCE_DIR}/src)
add_library(pyplasma SHARED pyplasma.cpp)
add_dependencies(pyplasma plasma)
set_target_properties(pyplasma PROPERTIES PREFIX "")

set_target_properties(pyplasma PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR})

target_link_libraries(pyplasma
        arrow_python
        plasma
        fmt::fmt
        pybind11::headers
)




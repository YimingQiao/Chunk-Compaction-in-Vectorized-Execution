cmake_minimum_required(VERSION 3.10)
project(Compaction-Project)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=native")

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif ()

add_executable(compaction
        main.cpp
        profiler.h
        base.cpp
        hash_table.cpp
        data_collection.cpp
        compactor.cpp)

# filter operator
add_executable(filter filter_main.cpp
        profiler.h
        base.cpp
        data_collection.cpp
filter_operator.h)

# If you have any libraries, you can link them like this:
# target_link_libraries(YourProjectName your_library)
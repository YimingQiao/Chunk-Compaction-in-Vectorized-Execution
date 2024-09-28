cmake_minimum_required(VERSION 3.10)
project(Compaction-Project)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=native")

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif ()

# add options for each compaction setting
option(USE_NO_COMPACT "Use Logical + No compact method" OFF)
option(USE_DYNAMIC_COMPACT "Use logical + dynamic compact method" OFF)

# check which compaction method was chosen
if (USE_NO_COMPACT)
    add_definitions(-Dflag_no_compact)
elseif (USE_DYNAMIC_COMPACT)
    add_definitions(-Dflag_dynamic_compact)
else ()
    # default to logical_compact
    add_definitions(-Dflag_no_compact)
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

# a pipeline starts with a filter operator.
add_executable(filter_and_join
        filters_and_joins.cpp
        profiler.h
        base.cpp
        hash_table.cpp
        compactor.cpp
        data_collection.cpp
        filter_operator.h
        negative_feedback.hpp)

# If you have any libraries, you can link them like this:
# target_link_libraries(YourProjectName your_library)
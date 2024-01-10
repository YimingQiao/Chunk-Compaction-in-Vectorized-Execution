cmake_minimum_required(VERSION 3.10)
project(Compaction-Project)

set(CMAKE_CXX_STANDARD 17)

add_executable(compaction main.cpp
        base.h
        hash_table.cpp
        data_collection.cpp)

# If you have any libraries, you can link them like this:
# target_link_libraries(YourProjectName your_library)
#!/bin/bash

# Create compaction directory, -p option makes it ignore if the directory already exists
mkdir -p compaction

# Project name - replace with your executable name
executables=("filter_and_join" "compaction")

for name in "${executables[@]}"; do
    # Build the no_compact version
    mkdir -p build-logical
    cd build-logical
    # Generate make files with all compaction options off (falls back to no-compact)
    cmake ..
    # Build the project
    make -j96
    # Move the project
    mv ${name} ../${name}/exe_logical_${name}
    # Return to parent directory
    cd ..
    rm -rf build-logical
done


#!/bin/bash

# Create compaction directory, -p option makes it ignore if the directory already exists
mkdir -p compaction
mkdir -p filter_and_join

# Dictionary of all compaction options
declare -A compaction_options=(
    ["logical"]="USE_NO_COMPACT"
    ["smart"]="USE_DYNAMIC_COMPACT")

# Project name - replace with your executable name
executables=("filter_and_join" "compaction")

for name in "${executables[@]}"; do
    for key in "${!compaction_options[@]}"; do
        # Build the no_compact version
        mkdir -p build-${key}
        cd build-${key}
        # Generate make files with the option enabled
        cmake -D${compaction_options[$key]}=ON ..
        # Generate make files with all compaction options off (falls back to no-compact)
        cmake ..
        # Build the project
        make -j96
        # Move the project
        mv ${name} ../${name}/exe_${key}_${name}
        # Return to parent directory
        cd ..
        rm -rf build-${key}
    done
done



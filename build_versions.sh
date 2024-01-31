#!/bin/bash

# Create compaction directory, -p option makes it ignore if the directory already exists
mkdir -p compaction

# Dictionary of all compaction options
declare -A compaction_options=(
    ["full"]="USE_FULL_COMPACT"
    ["binary"]="USE_BINARY_COMPACT"
    ["dynamic"]="USE_DYNAMIC_COMPACT")

# Project name - replace with your executable name
project_name="compaction"

# Loop through all compaction options and compile projects
for key in ${!compaction_options[@]}; do
    # Create a unique build directory for each option
    mkdir -p build-${key}
    cd build-${key}
    # Generate make files with the option enabled
    cmake -D${compaction_options[$key]}=ON ..
    # Build the project
    make -j96
    # Move the project
    mv ${project_name} ../compaction/exe_${key}_${project_name}
    # Return to parent directory
    cd ..
    rm -rf build-${key}
done

# Build the no_compact version
mkdir -p build-no
cd build-no
# Generate make files with all compaction options off (falls back to no-compact)
cmake ..
# Build the project
make -j96
# Move the project
mv ${project_name} ../compaction/exe_no_${project_name}
# Return to parent directory
cd ..
rm -rf build-no
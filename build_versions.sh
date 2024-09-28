#!/bin/bash

# Create compaction directory, -p option makes it ignore if the directory already exists
mkdir -p compaction
mkdir -p filter_and_join

# Dictionary of all compaction options
declare -A compaction_options=(
    ["full"]="USE_FULL_COMPACT"
    ["binary"]="USE_BINARY_COMPACT"
    ["dynamic"]="USE_DYNAMIC_COMPACT")

# Define project names
executables=("filter_and_join" "compaction")

# Loop through all compaction options and compile projects
for name in "${executables[@]}"; do
    for key in "${!compaction_options[@]}"; do
        # Create a unique build directory for each option
        mkdir -p build-${key}-${name}
        cd build-${key}-${name}
        # Generate make files with the option enabled
        cmake -D${compaction_options[$key]}=ON ..
        # Build the project
        make -j96
        # Move the project
        mv ${name} ../${name}/exe_${key}_${name}
        # Return to parent directory
        cd ..
        rm -rf build-${key}-${name}
    done

    # Build the no_compact version
    mkdir -p build-no-${name}
    cd build-no-${name}
    # Generate make files with all compaction options off (falls back to no-compact)
    cmake ..
    # Build the project
    make -j96
    # Move the project
    mv ${name} ../${name}/exe_no_${name}
    # Return to parent directory
    cd ..
    rm -rf build-no-${name}
done

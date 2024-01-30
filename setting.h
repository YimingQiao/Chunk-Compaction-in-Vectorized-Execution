//===----------------------------------------------------------------------===//
//
//                         Compaction
//
// setting.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

// This file contains all parameters used in the project
namespace compaction {

// query setting
const size_t kJoins = 3;
const size_t kLHSTupleSize = 2e7;
const size_t kRHSTupleSize = 2e6;
const size_t kChunkFactor = 8;

// compaction setting
#define flag_no_compact

constexpr bool kEnableLogicalCompact = true;

// #define flag_collect_tuples

}
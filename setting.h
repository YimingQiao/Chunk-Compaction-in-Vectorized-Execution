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
const size_t kJoins = 4;
const size_t kLHSTupleSize = 2e7;
const size_t kRHSTupleSize = 2e6;
const size_t kChunkFactor = 6;
const vector<size_t> kRHSPayLoadLength{0, 1000, 0, 0};

// compaction setting
#define flag_dynamic_compact

#ifdef flag_full_compact
using Compactor = NaiveCompactor;
#elif defined(flag_binary_compact)
using Compactor = BinaryCompactor;
#elif defined(flag_dynamic_compact)
using Compactor = DynamicCompactor;
#elif defined(flag_no_compact)
using Compactor = NaiveCompactor;
#endif

// #define flag_collect_tuples

}
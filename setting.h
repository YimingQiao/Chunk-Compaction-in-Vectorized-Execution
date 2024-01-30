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
size_t kJoins = 4;
size_t kLHSTupleSize = 2e7;
size_t kRHSTupleSize = 2e6;
size_t kChunkFactor = 6;
vector<size_t> kRHSPayLoadLength{0, 0, 0, 0};

// compaction setting
#define flag_no_compact

#ifdef flag_full_compact
using Compactor = NaiveCompactor;
#elif defined(flag_binary_compact)
using Compactor = BinaryCompactor;
#elif defined(flag_dynamic_compact)
using Compactor = DynamicCompactor;
#elif defined(flag_no_compact)
using Compactor = NaiveCompactor;
#endif

bool flag_collect_tuples = false;
}
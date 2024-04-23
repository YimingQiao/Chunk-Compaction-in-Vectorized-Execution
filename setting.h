//===----------------------------------------------------------------------===//
//
//                         Compaction
//
// setting.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "compactor.h"

// This file contains all parameters used in the project
namespace compaction {

// join query setting
size_t kJoins = 4;
size_t kLHSTupleSize = 2e7;
size_t kRHSTupleSize = 2e6;
size_t kChunkFactor = 6;
double kLoadFactor = 0.5;
vector<size_t> kRHSPayLoadLength{0, 0, 0, 0};

// filter setting
size_t kFilter = 1;
size_t kTupleSize = 2e7;
size_t kCols = 10;
double kSelectivity = 0.2;

// compaction setting
#ifdef flag_full_compact
using Compactor = NaiveCompactor;
const string strategy_name = "full_compaction";
#elif defined(flag_binary_compact)
using Compactor = BinaryCompactor;
const string strategy_name = "binary_compaction";
#elif defined(flag_dynamic_compact)
using Compactor = DynamicCompactor;
const string strategy_name = "dynamic_compaction";
#else
using Compactor = NaiveCompactor;
const string strategy_name = "no_compaction";
#endif

bool flag_collect_tuples = false;


// filter setting
}
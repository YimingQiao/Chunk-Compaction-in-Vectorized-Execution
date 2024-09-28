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
vector<size_t> kRHSPayLoadLength{0, 1000, 0, 0};
size_t kLHSTupleSize = 2e7;
size_t kRHSTupleSize = 2e6;
size_t kChunkFactor = 8;
double kLoadFactor = 0.5;

// filter setting
size_t kFilter = 1;
size_t kTupleSize = 2e7;
size_t kCols = 10;
double kSelectivity = 0.2;

// #define flag_dynamic_compact

#if defined(flag_full_compact)
using Compactor = NaiveCompactor;
const string strategy_name = "full_compaction";
#elif defined(flag_dynamic_compact)
using Compactor = DynamicCompactor;
const string strategy_name = "dynamic_compaction";
#else
using Compactor = NaiveCompactor;
const string strategy_name = "no_compaction";
#endif

constexpr bool kEnableLogicalCompact = true;

bool flag_collect_tuples = false;
}
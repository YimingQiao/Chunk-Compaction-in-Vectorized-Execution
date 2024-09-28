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

#ifdef defined(flag_dynamic_compact)
using Compactor = DynamicCompactor;
const string strategy_name = "dynamic_compaction";
#endif

constexpr bool kEnableLogicalCompact = true;

bool flag_collect_tuples = false;
}
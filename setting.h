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
vector<size_t> kRHSPayLoadLength{0, 1000, 0, 0};
size_t kLHSTupleSize = 2e7;
size_t kRHSTupleSize = 2e6;
size_t kChunkFactor = 1;

constexpr bool kEnableLogicalCompact = true;

bool flag_collect_tuples = false;
}
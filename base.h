//===----------------------------------------------------------------------===//
//
//                         Compaction
//
// base.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

constexpr size_t kBlockSize = 2048;


using Attribute = std::variant<size_t, double, std::string>;

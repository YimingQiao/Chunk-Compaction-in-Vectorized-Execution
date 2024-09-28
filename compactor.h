//===----------------------------------------------------------------------===//
//                         Compaction
//
// compactor.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "base.h"
#include "profiler.h"
#include "negative_feedback.hpp"

namespace compaction {
class NaiveCompactor {
 public:
  explicit NaiveCompactor(vector<AttributeType> &types)
      : cached_chunk_(std::make_unique<DataChunk>(types)),
        temp_chunk_(std::make_unique<DataChunk>(types)),
        name_("0x" + std::to_string(size_t(this))) {}

  void Compact(unique_ptr<DataChunk> &chunk);

  inline void Flush(unique_ptr<DataChunk> &chunk) { chunk = std::move(cached_chunk_); }

 private:
  unique_ptr<DataChunk> cached_chunk_;
  unique_ptr<DataChunk> temp_chunk_;
  const string name_;
};

class DynamicCompactor {
 public:
  void SetThreshold(size_t threshold) { compact_threshold_ = threshold; }

  size_t GetThreshold() const { return compact_threshold_; }

 public:
  explicit DynamicCompactor(const vector<AttributeType> &types)
      : cached_chunk_(std::make_unique<DataChunk>(types)),
        name_("[Dynamic Compact] 0x" + std::to_string(size_t(this))) {}

  void Compact(unique_ptr<DataChunk> &chunk);

  inline void Flush(unique_ptr<DataChunk> &chunk) {
    chunk = std::move(cached_chunk_);
  }

 private:
  size_t compact_threshold_ = 128;
  unique_ptr<DataChunk> cached_chunk_;

  const string name_;
  Profiler profiler_;
};
}

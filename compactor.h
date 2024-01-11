//===----------------------------------------------------------------------===//
//                         Compaction
//
// compactor.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "base.h"

namespace compaction {
class NaiveCompactor {
 public:
  explicit NaiveCompactor(vector<AttributeType> &types)
      : cached_chunk_(std::make_unique<DataChunk>(types)), temp_chunk_(std::make_unique<DataChunk>(types)) {}

  void Compact(unique_ptr<DataChunk> &chunk);

  inline void Flush(unique_ptr<DataChunk> &chunk) { chunk = std::move(cached_chunk_); }

 private:
  unique_ptr<DataChunk> cached_chunk_;
  unique_ptr<DataChunk> temp_chunk_;
};
}

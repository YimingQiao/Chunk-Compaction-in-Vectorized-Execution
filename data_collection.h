//===----------------------------------------------------------------------===//
//
//                         Compaction
//
// data_collection.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "base.h"

namespace compaction {
class DataCollection {
 public:
  explicit DataCollection(vector<AttributeType> &types) : types_(types), n_tuples_(0) {}

  void AppendTuple(vector<Attribute> &tuple);

  void AppendChunk(DataChunk &chunk);

  DataChunk FetchChunk(size_t start, size_t end);

  inline size_t NumTuples() const { return n_tuples_; }

  void Print(size_t n_tuple);

 private:
  vector<AttributeType> types_;
  size_t n_tuples_;
  vector<vector<Attribute>> collection_;
};
}

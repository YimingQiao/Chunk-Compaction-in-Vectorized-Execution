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
  explicit DataCollection(vector<AttributeType> &types) : types_(types), n_tuples(0) {}

  void AppendTuple(vector<Attribute> &tuple) {
    collection_.push_back(tuple);
    ++n_tuples;
  }

  void AppendChunk(DataChunk &chunk) {
    assert(types_ == chunk.types_);
    vector<Attribute> tuple(types_.size());
    for (size_t i = 0; i < chunk.count_; ++i) {
      for (size_t j = 0; j < tuple.size(); ++j) {
        auto idx = chunk.data_[j].selection_vector_[i];
        tuple[j] = chunk.data_[j].GetValue(idx);
      }
      collection_.push_back(tuple);
    }
    n_tuples += chunk.count_;
  }

  DataChunk FetchChunk(size_t start, size_t end) {
    DataChunk chunk(types_);
    for (size_t i = start; i < end; ++i) {
      chunk.AppendTuple(collection_[i]);
    }
    return chunk;
  }

  void Print() {
    for (size_t i = 0; i < n_tuples; ++i) {
      auto &tuple = collection_[i];
      for (size_t j = 0; j < tuple.size(); ++j) {
        switch (types_[j]) {
          case AttributeType::INTEGER: {
            std::cout << std::get<size_t>(tuple[j]) << ", ";
            break;
          }
          case AttributeType::DOUBLE: {
            std::cout << std::get<double>(tuple[j]) << ", ";
            break;
          }
          case AttributeType::STRING: {
            std::cout << std::get<std::string>(tuple[j]) << ", ";
            break;
          }
          case AttributeType::INVALID:break;
        }
      }
      std::cout << "\n";
    }
  }

 private:
  vector<AttributeType> types_;
  size_t n_tuples;
  vector<vector<Attribute>> collection_;
};
}

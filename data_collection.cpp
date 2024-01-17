
#include "data_collection.h"

namespace compaction {
void compaction::DataCollection::AppendTuple(std::vector<compaction::Attribute> &tuple) {
  collection_.push_back(tuple);
  ++n_tuples_;
}

void compaction::DataCollection::AppendChunk(compaction::DataChunk &chunk) {
  assert(types_ == chunk.types_);
  collection_.resize(chunk.count_ + n_tuples_);

  vector<Attribute> tuple(types_.size());
  for (size_t i = 0; i < chunk.count_; ++i) {
    auto idx = chunk.selection_vector_[i];
    for (size_t j = 0; j < tuple.size(); ++j) {
      tuple[j] = chunk.data_[j].GetValue(idx);
    }
    collection_[n_tuples_ + i] = tuple;
  }
  n_tuples_ += chunk.count_;
}

compaction::DataChunk compaction::DataCollection::FetchChunk(size_t start, size_t end) {
  DataChunk chunk(types_);
  for (size_t i = start; i < end; ++i) {
    chunk.AppendTuple(collection_[i]);
  }
  return chunk;
}

void compaction::DataCollection::Print(size_t n_tuple) {
  n_tuple = std::min(n_tuple, n_tuples_);

  for (size_t i = 0; i < n_tuple; ++i) {
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
}
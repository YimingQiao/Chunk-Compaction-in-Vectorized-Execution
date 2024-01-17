
#include "base.h"

namespace compaction {

void Vector::Reference(Vector &other) {
  assert(type_ == other.type_);
  data_ = other.data_;
}

DataChunk::DataChunk(const vector<AttributeType> &types) : count_(0), types_(types), selection_vector_(kBlockSize) {
  for (auto &type : types) data_.emplace_back(type);
  for (size_t i = 0; i < kBlockSize; ++i) selection_vector_[i] = i;
}

void DataChunk::Append(DataChunk &chunk, size_t num, size_t offset) {
  assert(types_.size() == chunk.types_.size());
  assert(count_ + num <= kBlockSize);

  for (size_t i = 0; i < types_.size(); ++i) {
    assert(types_[i] == chunk.types_[i]);
    for (size_t j = 0; j < num; ++j) {
      auto r_idx = chunk.selection_vector_[j + offset];
      data_[i].GetValue(count_ + j) = chunk.data_[i].GetValue(r_idx);
    }
  }
  count_ += num;
}

void DataChunk::AppendTuple(vector<Attribute> &tuple) {
  for (size_t i = 0; i < types_.size(); ++i) {
    auto &col = data_[i];
    col.GetValue(count_) = tuple[i];
  }
  ++count_;
}

void DataChunk::Slice(DataChunk &other, vector<uint32_t> &selection_vector, size_t count) {
  assert(other.data_.size() <= data_.size());
  this->count_ = count;
  for (size_t c = 0; c < other.data_.size(); ++c) {
    data_[c].Reference(other.data_[c]);
  }

  selection_vector_ = other.selection_vector_;
  for (size_t i = 0; i < count; ++i) {
    auto new_idx = selection_vector[i];
    auto key_idx = selection_vector_[new_idx];
    selection_vector_[i] = key_idx;
  }
}

void DataChunk::Print() {
  for (size_t i = 0; i < count_; ++i) {
    for (size_t j = 0; j < data_.size(); ++j) {
      size_t idx = selection_vector_[i];
      switch (types_[j]) {
        case AttributeType::INTEGER: {
          std::cout << std::get<size_t>(data_[j].GetValue(idx)) << ", ";
          break;
        }
        case AttributeType::DOUBLE: {
          std::cout << std::get<double>(data_[j].GetValue(idx)) << ", ";
          break;
        }
        case AttributeType::STRING: {
          std::cout << std::get<std::string>(data_[j].GetValue(idx)) << ", ";
          break;
        }
        case AttributeType::INVALID:break;
      }
    }
    std::cout << "\n";
  }
  std::cout << "-------------------------------\n";
}
}

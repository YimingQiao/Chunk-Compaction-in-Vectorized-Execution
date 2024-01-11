
#include "base.h"

namespace compaction {

Vector::Vector(AttributeType type) : type_(type), count_(0), data_(std::make_shared<vector<Attribute>>(kBlockSize)),
                                     selection_vector_(kBlockSize) {
  for (size_t i = 0; i < kBlockSize; ++i) selection_vector_[i] = i;
}

void Vector::Append(Vector &other, size_t num, size_t offset) {
  assert(count_ + num <= kBlockSize);
  // current selection vector = [0, 1, 2, ..., count_ - 1]
  for (size_t i = 0; i < num; ++i) {
    auto r_idx = other.selection_vector_[i + offset];
    GetValue(count_++) = other.GetValue(r_idx);
  }
}

void Vector::Slice(vector<uint32_t> &selection_vector, size_t count) {
  count_ = count;
  for (size_t i = 0; i < count; ++i) {
    auto new_idx = selection_vector[i];
    auto key_idx = selection_vector_[new_idx];
    selection_vector_[i] = key_idx;
  }
}

void Vector::Reference(Vector &other) {
  assert(type_ == other.type_);
  data_ = other.data_;
  selection_vector_ = other.selection_vector_;
}

DataChunk::DataChunk(const vector<AttributeType> &types) : count_(0), types_(types) {
  for (auto &type : types) data_.emplace_back(type);
}

void DataChunk::Append(DataChunk &chunk, size_t num, size_t offset) {
  assert(types_.size() == chunk.types_.size());
  assert(count_ + num <= kBlockSize);

  for (size_t i = 0; i < types_.size(); ++i) {
    assert(types_[i] == chunk.types_[i]);
    data_[i].Append(chunk.data_[i], num, offset);
  }
  count_ += num;
}

void DataChunk::AppendTuple(vector<Attribute> &tuple) {
  for (size_t i = 0; i < types_.size(); ++i) {
    auto &col = data_[i];
    col.GetValue(col.count_++) = tuple[i];
  }
  ++count_;
}

void DataChunk::Slice(DataChunk &other, vector<uint32_t> &selection_vector, size_t count) {
  assert(other.data_.size() <= data_.size());
  this->count_ = count;
  for (size_t c = 0; c < other.data_.size(); ++c) {
    data_[c].Reference(other.data_[c]);
    data_[c].Slice(selection_vector, count);
  }
}

void DataChunk::Print() {
  for (size_t i = 0; i < count_; ++i) {
    for (size_t j = 0; j < data_.size(); ++j) {
      size_t idx = data_[j].selection_vector_[i];
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

//===----------------------------------------------------------------------===//
//
//                         Compaction
//
// base.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>
#include <variant>
#include <iostream>
#include <cassert>
#include <list>
#include <unordered_map>

namespace compaction {
// Some data structures
using std::vector;
using std::list;
using std::shared_ptr;
using std::unique_ptr;
using std::unordered_map;
using std::string;

// Each block here is 2048
constexpr size_t kBlockSize = 2048;

// Attribute includes three types: integer, float-point number, and the string.
using Attribute = std::variant<size_t, double, std::string>;

enum class AttributeType : uint8_t {
  INTEGER = 0,
  DOUBLE = 1,
  STRING = 2,
  INVALID = 3
};

// The vector uses Row ID.
class Vector {
 public:
  AttributeType type_;
  size_t count_;
  vector<uint32_t> selection_vector_;

  explicit Vector(AttributeType type) : type_(type), count_(0), data_(std::make_shared<vector<Attribute>>(kBlockSize)),
                                        selection_vector_(kBlockSize) {
    for (size_t i = 0; i < kBlockSize; ++i) selection_vector_[i] = i;
  }

  void Slice(vector<uint32_t> &selection_vector, size_t count) {
    count_ = count;
    for (size_t i = 0; i < count; ++i) {
      auto new_idx = selection_vector[i];
      auto idx = selection_vector_[new_idx];
      selection_vector_[i] = idx;
    }
  }

  void Reference(Vector &other) {
    assert(type_ == other.type_);
    data_ = other.data_;
    selection_vector_ = other.selection_vector_;
  }

  Attribute &GetValue(size_t idx) {
    return (*data_)[idx];
  }

 private:
  shared_ptr<vector<Attribute>> data_;
};

// A data chunk has some columns.
class DataChunk {
 public:
  size_t count_;
  vector<Vector> data_;
  vector<AttributeType> types_;

  explicit DataChunk(const vector<AttributeType> &types) : count_(0), types_(types) {
    for (auto &type : types) data_.emplace_back(type);
  }

  inline void AppendTuple(vector<Attribute> &tuple) {
    for (size_t i = 0; i < types_.size(); ++i) {
      auto &col = data_[i];
      col.GetValue(col.count_++) = tuple[i];
    }
    ++count_;
  }

  inline void Slice(DataChunk &other, vector<uint32_t> &selection_vector, size_t count) {
    assert(other.data_.size() <= data_.size());
    this->count_ = count;
    for (size_t c = 0; c < other.data_.size(); ++c) {
      data_[c].Reference(other.data_[c]);
      data_[c].Slice(selection_vector, count);
    }
  }

  void Print() {
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
};
}
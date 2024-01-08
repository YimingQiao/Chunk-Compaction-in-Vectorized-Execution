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

namespace compaction {
// Some data structures
template<typename T>
using vector = std::vector<T>;

template<typename T>
using list = std::list<T>;

template<typename T>
using shared_ptr = std::shared_ptr<T>;

template<typename T>
using shared_ptr = std::shared_ptr<T>;

template<typename K, typename V>
using unordered_map = std::unordered_map<K, V>;

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

// The vector based on Row ID design.
class Vector {
 public:
  explicit Vector(AttributeType type) : type_(type), data_(std::make_shared<vector<Attribute>>(kBlockSize)),
                                        selection_vector_(kBlockSize) {
    for (size_t i = 0; i < kBlockSize; ++i) selection_vector_[i] = i;
  }

  void Slice(const vector<uint32_t> &selection_vector, size_t count) {
    for (size_t i = 0; i < count; ++i) {
      auto new_idx = selection_vector[i];
      auto idx = selection_vector_[new_idx];
      selection_vector_[i] = idx;
    }
  }

 private:
  AttributeType type_;
  shared_ptr<vector<Attribute>> data_;
  vector<uint32_t> selection_vector_;
};

// A data chunk has some columns.
class DataChunk {
 public:
  explicit DataChunk(const vector<AttributeType> &types) : count_(0) {
    for (auto &type : types) data_.emplace_back(type);
  }

  vector<Vector> data_;
 private:
  size_t count_;
};

// A tuple in hash table
class Tuple {
  vector<Attribute> values;
};
}
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

  explicit Vector(AttributeType type) : type_(type), data_(std::make_shared<vector<Attribute>>(kBlockSize)) {}

  inline void Reference(Vector &other);

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
  vector<uint32_t> selection_vector_;

  explicit DataChunk(const vector<AttributeType> &types);

  void Append(DataChunk &chunk, size_t num, size_t offset = 0);

  void AppendTuple(vector<Attribute> &tuple);

  void Slice(DataChunk &other, vector<uint32_t> &selection_vector, size_t count);
};
}
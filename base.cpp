
#include "base.h"

namespace compaction {

void Vector::Append(Vector &other, size_t num, size_t offset) {
  assert(count_ + num <= kBlockSize);
  // current selection vector = [0, 1, 2, ..., count_ - 1]
  for (size_t i = 0; i < num; ++i) {
    auto r_idx = other.selection_vector_[i + offset];
    GetValue(count_++) = other.GetValue(r_idx);
  }
}

void Vector::Slice(Vector &other, vector<uint32_t> &selection_vector, size_t count) {
  for (size_t i = 0; i < count; ++i) {
    auto new_idx = selection_vector[i];
    auto key_idx = other.selection_vector_[new_idx];
    selection_vector_[i + count_] = key_idx;
  }
  count_ += count;
}

//! This assigns a shared pointer, but ONLY assigns if "target" is not equal to "source"
//! If this is often the case, this manner of assignment is significantly faster (~20X faster)
//! Since it avoids the need of an atomic incref/decref at the cost of a single pointer comparison
//! Benchmark: https://gist.github.com/Mytherin/4db3faa8e233c4a9b874b21f62bb4b96
//! If the shared pointers are not the same, the penalty is very low (on the order of 1%~ slower)
//! This method should always be preferred if there is a (reasonable) chance that the pointers are the same
template<class T>
void AssignSharedPointer(shared_ptr<T> &target, const shared_ptr<T> &source) {
  if (target.get() != source.get()) {
    target = source;
  }
}

void Vector::Reference(Vector &other) {
  assert(type_ == other.type_);
  AssignSharedPointer(data_, other.data_);
  // data_ = other.data_;
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
  for (size_t c = 0; c < other.data_.size(); ++c) {
    data_[c].Reference(other.data_[c]);
    data_[c].Slice(other.data_[c], selection_vector, count);
  }
  this->count_ += count;
}
}

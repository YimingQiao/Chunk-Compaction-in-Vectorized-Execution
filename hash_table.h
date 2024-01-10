//===----------------------------------------------------------------------===//
//
//                         Compaction
//
// hash_table
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <unordered_map>
#include <functional>
#include <utility>

#include "base.h"

namespace compaction {
struct Tuple {
  vector<Attribute> attrs_;
};

class ScanStructure {
 public:
  explicit ScanStructure(size_t count, vector<list<Tuple> *> buckets, vector<uint32_t> sel_vector)
      : count_(count), buckets_(std::move(buckets)), sel_vector_(std::move(sel_vector)) {
    iterators_.resize(kBlockSize);
    for (size_t i = 0; i < count; ++i) {
      auto idx = sel_vector_[i];
      iterators_[idx] = buckets_[idx]->begin();
    }
  }

  void Next(Vector &join_key, DataChunk &input, DataChunk &result);

  bool HasNext() const { return count_ > 0; }

 private:
  size_t count_;
  vector<list<Tuple> *> buckets_;
  vector<uint32_t> sel_vector_;

  vector<list<Tuple>::iterator> iterators_;

  size_t ScanInnerJoin(Vector &join_key, vector<uint32_t> &result_vector);

  inline void AdvancePointers();

  inline void GatherResult(Vector &column, vector<uint32_t> &sel_vector, size_t count);
};

class HashTable {
 public:
  HashTable(size_t n_rhs_tuples, size_t chunk_factor);

  ScanStructure Probe(Vector &join_key);

 private:
  size_t n_buckets_;
  vector<unique_ptr<list<Tuple>>> linked_lists_;
  std::hash<Attribute> hash_;
};
}

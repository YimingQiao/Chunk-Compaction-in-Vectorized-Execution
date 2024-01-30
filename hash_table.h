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
#include "profiler.h"

namespace compaction {

class HashTable;

struct Tuple {
  vector<Attribute> attrs_;
};

class ScanStructure {
 public:
  explicit ScanStructure(size_t count,
                         vector<uint32_t> bucket_sel_vector,
                         vector<list<Tuple> *> buckets,
                         vector<uint32_t> &key_sel_vector,
                         HashTable *ht)
      : count_(count), buckets_(std::move(buckets)),
        bucket_sel_vector_(std::move(bucket_sel_vector)), key_sel_vector_(key_sel_vector), ht_(ht) {
    iterators_.resize(kBlockSize);
    for (size_t i = 0; i < count; ++i) {
      auto idx = bucket_sel_vector_[i];
      iterators_[idx] = buckets_[idx]->begin();
    }
  }

  void Next(Vector &join_key, DataChunk &input, DataChunk &result, bool compact_mode = true);

  inline bool HasNext() const { return HasBucket() || HasBuffer(); }

 private:
  size_t count_;
  vector<list<Tuple> *> buckets_;
  vector<uint32_t> bucket_sel_vector_;
  vector<uint32_t> &key_sel_vector_;
  vector<list<Tuple>::iterator> iterators_;
  HashTable *ht_;

  // buffer
  unique_ptr<DataChunk> buffer_;

  size_t ScanInnerJoin(Vector &join_key, vector<uint32_t> &result_vector);

  inline void AdvancePointers();

  inline void GatherResult(vector<Vector *> cols, vector<uint32_t> &result_vector, size_t count);

  inline bool HasBucket() const { return count_ > 0; }

  inline bool HasBuffer() const { return buffer_ != nullptr && buffer_->count_ > 0; }

  void NextInternal(Vector &join_key, DataChunk &input, DataChunk &result);
};

class HashTable {
 public:
  HashTable(size_t n_rhs_tuples, size_t chunk_factor, size_t payload_length);

  ScanStructure Probe(Vector &join_key);

 private:
  size_t n_buckets_;
  vector<unique_ptr<list<Tuple>>> linked_lists_;
  std::hash<Attribute> hash_;
};
}

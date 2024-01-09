#include "hash_table.h"

namespace compaction {
HashTable::HashTable(size_t n_rhs_tuples, size_t chunk_factor) {
  n_buckets_ = 2 * n_rhs_tuples;
  linked_lists_.resize(n_buckets_);
  for (auto &bucket : linked_lists_) bucket = std::make_unique<list<Attribute>>();

  // join key
  vector<size_t> column(n_rhs_tuples);
  for (size_t i = 0; i < n_rhs_tuples; ++i) {
    column[i] = i * chunk_factor % n_rhs_tuples;
  }

  // build hash table
  for (size_t i = 0; i < n_rhs_tuples; ++i) {
    Attribute value = column[i];
    auto bucket_idx = hash_(value) % n_buckets_;
    auto &bucket = linked_lists_[bucket_idx];
    bucket->push_back(value);
  }
}

ScanStructure HashTable::Probe(Vector &join_key) {
  vector<list<Attribute> *> ptrs(join_key.selection_vector_.size());
  for (size_t i = 0; i < join_key.count_; ++i) {
    auto idx = join_key.selection_vector_[i];
    auto attr = join_key.GetValue(idx);
    auto bucket_idx = hash_(attr) % n_buckets_;
    ptrs[idx] = linked_lists_[bucket_idx].get();
  }

  size_t n_non_empty = 0;
  vector<uint32_t> selection_vector(kBlockSize);
  for (size_t i = 0; i < join_key.count_; ++i) {
    auto idx = join_key.selection_vector_[i];
    if (!ptrs[idx]->empty()) {
      selection_vector[n_non_empty] = idx;
      ++n_non_empty;
    }
  }
  return ScanStructure(n_non_empty, ptrs, selection_vector);
}

void ScanStructure::Next(Vector &join_key, DataChunk &input, DataChunk &result) {
  if (count_ == 0) {
    // no pointers left to chase
    return;
  }

  vector<uint32_t> result_vector(kBlockSize);
  size_t result_count = ScanInnerJoin(join_key, result_vector);

  if (result_count > 0) {
    // matches were found
    // construct the result
    // on the LHS, we create a slice using the result vector
    result.Slice(input, result_vector, result_count);

    // on the RHS, we need to fetch the data from the hash table
    auto &vector = result.data_[input.data_.size()];
    GatherResult(vector, result_vector, result_count);
  }
  AdvancePointers();
}

size_t ScanStructure::ScanInnerJoin(Vector &join_key, vector<uint32_t> &result_vector) {
  while (true) {
    // Match
    size_t result_count = 0;
    result_vector = sel_vector_;
    for (size_t i = 0; i < count_; ++i) {
      auto idx = result_vector[i];
      if (join_key.GetValue(idx) == *iterators_[idx]) {
        result_vector[result_count++] = idx;
      }
    }

    if (result_count > 0) {
      return result_count;
    }

    // no matches found: check the next set of pointers
    AdvancePointers();
    if (count_ == 0) {
      return 0;
    }
  }
}

void ScanStructure::AdvancePointers() {
  size_t new_count = 0;
  for (size_t i = 0; i < count_; i++) {
    auto idx = sel_vector_[i];
    if (++iterators_[idx] != buckets_[idx]->end()) {
      sel_vector_[new_count++] = idx;
    }
  }
  count_ = new_count;
}

void ScanStructure::GatherResult(Vector &column, vector<uint32_t> &sel_vector, size_t count) {
  column.count_ = count;
  for (size_t i = 0; i < count; ++i) {
    auto idx = sel_vector[i];
    column.GetValue(i) = *iterators_[idx];
  }
}
}

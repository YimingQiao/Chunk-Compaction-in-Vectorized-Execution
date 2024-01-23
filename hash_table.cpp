#include "hash_table.h"

namespace compaction {
HashTable::HashTable(size_t n_rhs_tuples, size_t chunk_factor) {
  n_buckets_ = 2 * n_rhs_tuples;
  linked_lists_.resize(n_buckets_);
  for (auto &bucket : linked_lists_) bucket = std::make_unique<list<Tuple>>();

  // Tuple in Hash Table
  string payload_name = "payload_0x" + std::to_string(size_t(this)) + "_";
  // string payload_name = "";
  vector<Tuple> rhs_table(n_rhs_tuples);
  for (size_t i = 0; i < n_rhs_tuples; ++i) {
    auto key = i * chunk_factor % n_rhs_tuples;
    auto payload = payload_name + std::to_string(i) + "|";
    rhs_table[i].attrs_.emplace_back(key);
    rhs_table[i].attrs_.emplace_back(payload);
  }

  // build hash table
  for (size_t i = 0; i < n_rhs_tuples; ++i) {
    auto &tuple = rhs_table[i];
    Attribute value = tuple.attrs_[0];
    auto bucket_idx = hash_(value) % n_buckets_;
    auto &bucket = linked_lists_[bucket_idx];
    bucket->push_back(tuple);
  }
}

ScanStructure HashTable::Probe(Vector &join_key, size_t count, vector<uint32_t> &sel_vector) {
  Profiler profiler;
  profiler.Start();

  vector<list<Tuple> *> ptrs(kBlockSize);
  for (size_t i = 0; i < count; ++i) {
    auto idx = sel_vector[i];
    auto attr = join_key.GetValue(idx);
    auto bucket_idx = hash_(attr) % n_buckets_;
    ptrs[idx] = linked_lists_[bucket_idx].get();
  }

  size_t n_non_empty = 0;
  vector<uint32_t> ptrs_sel_vector(kBlockSize);
  for (size_t i = 0; i < count; ++i) {
    auto idx = sel_vector[i];
    if (!ptrs[idx]->empty()) ptrs_sel_vector[n_non_empty++] = i;
  }
  auto ret = ScanStructure(n_non_empty, ptrs_sel_vector, ptrs, sel_vector, this);

  double time = profiler.Elapsed();
  BeeProfiler::Get().InsertStatRecord("[Join - Probe] 0x" + std::to_string(size_t(this)), time);
  ZebraProfiler::Get().InsertRecord("[Join - Probe] 0x" + std::to_string(size_t(this)), count, time);
  return ret;
}

void ScanStructure::Next(Vector &join_key, DataChunk &input, DataChunk &result) {
  if (count_ == 0) {
    // no pointers left to chase
    return;
  }

  Profiler profiler;
  profiler.Start();

  vector<uint32_t> result_vector(kBlockSize);
  size_t result_count = ScanInnerJoin(join_key, result_vector);

  if (result_count > 0) {
    // matches were found
    // construct the result
    // on the LHS, we create a slice using the result vector
    result.Slice(input, result_vector, result_count);

    // on the RHS, we need to fetch the data from the hash table
    vector<Vector *> cols{&result.data_[input.data_.size()], &result.data_[input.data_.size() + 1]};
    GatherResult(cols, input.selection_vector_, result_vector, result_count);
  }
  AdvancePointers();

  double time = profiler.Elapsed();
  BeeProfiler::Get().InsertStatRecord("[Join - Next] 0x" + std::to_string(size_t(ht_)), time);
  ZebraProfiler::Get().InsertRecord("[Join - Next] 0x" + std::to_string(size_t(ht_)), input.count_, time);
}

size_t ScanStructure::ScanInnerJoin(Vector &join_key, vector<uint32_t> &result_vector) {
  while (true) {
    // Match
    size_t result_count = 0;
    for (size_t i = 0; i < count_; ++i) {
      size_t idx = bucket_sel_vector_[i];
      size_t key_idx = bucket_format_[idx];
      auto &l_key = join_key.GetValue(key_idx);
      auto &r_key = iterators_[key_idx]->attrs_[0];
      if (l_key == r_key) result_vector[result_count++] = idx;
    }

    if (result_count > 0) return result_count;

    // no matches found: check the next set of pointers
    AdvancePointers();
    if (count_ == 0) return 0;
  }
}

void ScanStructure::AdvancePointers() {
  size_t new_count = 0;
  for (size_t i = 0; i < count_; i++) {
    auto idx = bucket_sel_vector_[i];
    auto key_idx = bucket_format_[idx];
    if (++iterators_[key_idx] != buckets_[key_idx]->end()) bucket_sel_vector_[new_count++] = idx;
  }
  count_ = new_count;
}

void ScanStructure::GatherResult(vector<Vector *> cols,
                                 vector<uint32_t> &sel_vector,
                                 vector<uint32_t> &result_vector,
                                 size_t count) {
  for (size_t c = 0; c < cols.size(); ++c) {
    auto &col = *cols[c];
    for (size_t i = 0; i < count; ++i) {
      auto idx = result_vector[i];
      auto r_idx = bucket_format_[idx];

      // columns from the right table align with the selection vector given by the left table
      auto l_idx = sel_vector[idx];
      col.GetValue(l_idx) = iterators_[r_idx]->attrs_[c];
    }
  }
}
}

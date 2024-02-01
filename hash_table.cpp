#include "hash_table.h"

namespace compaction {
HashTable::HashTable(size_t n_rhs_tuples, size_t chunk_factor, size_t payload_length) {
  n_buckets_ = 2 * n_rhs_tuples;
  linked_lists_.resize(n_buckets_);
  for (auto &bucket : linked_lists_) bucket = std::make_unique<list<Tuple>>();

  // Tuple in Hash Table
  string payload_name;
  if (payload_length > 0) {
    payload_name = "payload_";
    payload_name += string(payload_length, 'x');
    payload_name += "_";
  }
  vector<Tuple> rhs_table(n_rhs_tuples);
  size_t cnt = 0;
  const size_t num_unique = n_rhs_tuples / chunk_factor + (n_rhs_tuples % chunk_factor != 0);
  for (size_t i = 0; i < num_unique; ++i) {
    auto unique_value = i * (n_rhs_tuples / num_unique);
    for (size_t j = 0; j < chunk_factor && cnt < n_rhs_tuples; ++j) {
      auto payload = payload_name + std::to_string(cnt) + "|";
      rhs_table[cnt].attrs_.emplace_back(unique_value);
      rhs_table[cnt].attrs_.emplace_back(payload);
      ++cnt;
    }
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

ScanStructure HashTable::Probe(Vector &join_key) {
  Profiler profiler;
  profiler.Start();

  vector<list<Tuple> *> ptrs(kBlockSize);
  for (size_t i = 0; i < join_key.count_; ++i) {
    auto attr = join_key.GetValue(join_key.selection_vector_[i]);
    auto bucket_idx = hash_(attr) % n_buckets_;
    ptrs[i] = linked_lists_[bucket_idx].get();
  }

  size_t n_non_empty = 0;
  vector<uint32_t> ptrs_sel_vector(kBlockSize);
  for (size_t i = 0; i < join_key.count_; ++i) {
    if (!ptrs[i]->empty()) ptrs_sel_vector[n_non_empty++] = i;
  }
  auto ret = ScanStructure(n_non_empty, ptrs_sel_vector, ptrs, join_key.selection_vector_, this);

  double time = profiler.Elapsed();
  BeeProfiler::Get().InsertStatRecord("[Join - Probe] 0x" + std::to_string(size_t(this)), time);
  ZebraProfiler::Get().InsertRecord("[Join - Probe] 0x" + std::to_string(size_t(this)), join_key.count_, time);
  return ret;
}

void ScanStructure::Next(Vector &join_key, DataChunk &input, DataChunk &result, bool compact_mode) {
  // reset the result chunk
  result.Reset();

  if (compact_mode) {
    // take the buffer data if the buffer is not empty
    if (HasBuffer()) {
      result.data_.swap(buffer_->data_);
      std::swap(result.count_, buffer_->count_);
    }

    // compact result chunks without extra memory copy
    while (HasNext() && !HasBuffer()) {
      NextInternal(join_key, input, result);
    }
  } else {
    NextInternal(join_key, input, result);
  }
}

void ScanStructure::NextInternal(compaction::Vector &join_key,
                                 compaction::DataChunk &input,
                                 compaction::DataChunk &result) {
  if (count_ == 0) {
    // no pointers left to chase
    return;
  }

  Profiler profiler;
  profiler.Start();

  vector<uint32_t> result_vector(kBlockSize);
  size_t result_count = ScanInnerJoin(join_key, result_vector);

  if (result_count > 0) {
    if (result.count_ + result_count <= kBlockSize) {
      // matches were found
      // construct the result
      // on the LHS, we create a slice using the result vector
      result.Slice(input, result_vector, result_count);

      // on the RHS, we need to fetch the data from the hash table
      vector<Vector *> cols{&result.data_[input.data_.size()], &result.data_[input.data_.size() + 1]};
      GatherResult(cols, result_vector, result_count);
    } else {
      // init the buffer
      if (buffer_ == nullptr) buffer_ = std::make_unique<DataChunk>(result.types_);

      // buffer the result
      buffer_->Slice(input, result_vector, result_count);
      vector<Vector *> cols{&buffer_->data_[input.data_.size()], &buffer_->data_[input.data_.size() + 1]};
      GatherResult(cols, result_vector, result_count);
    }
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
      auto &l_key = join_key.GetValue(key_sel_vector_[idx]);
      auto &r_key = iterators_[idx]->attrs_[0];
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
    if (++iterators_[idx] != buckets_[idx]->end()) bucket_sel_vector_[new_count++] = idx;
  }
  count_ = new_count;
}

void ScanStructure::GatherResult(vector<Vector *> cols, vector<uint32_t> &sel_vector, size_t count) {
  for (size_t c = 0; c < cols.size(); ++c) {
    auto &col = *cols[c];
    for (size_t i = 0; i < count; ++i) {
      auto idx = sel_vector[i];
      col.GetValue(i + col.count_) = iterators_[idx]->attrs_[c];
    }
    col.count_ += count;
  }
}
}

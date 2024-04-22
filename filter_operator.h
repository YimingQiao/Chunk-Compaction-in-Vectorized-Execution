#pragma once

#include "base.h"

namespace compaction {

class FilterOperator {
 public:
  explicit FilterOperator(double selectivity) : selectivity_(selectivity), threshold_(100 * selectivity) {}

  void Execute(DataChunk &input, size_t col_id, DataChunk &result) const {
    result.Reset();

    auto &target_col = input.data_[col_id];

    size_t result_count = 0;
    vector<uint32_t> result_vector(kBlockSize);

    for (size_t i = 0; i < input.count_; i++) {
      size_t idx = target_col.selection_vector_[i];
      auto &value = target_col.GetValue(idx);
      if (CheckIfPass(value)) result_vector[result_count++] = i;
    }

    result.Slice(input, result_vector, result_count);
  }

  bool CheckIfPass(Attribute &value) const {
    size_t v = std::get<size_t>(value);

    return double(v) / 100 < selectivity_;
  }

 private:
  double selectivity_;
  int threshold_;
};
}

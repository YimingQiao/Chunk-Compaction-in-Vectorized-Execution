#pragma once

#include "base.h"

namespace compaction {

class FilterOperator {
 public:
  explicit FilterOperator(double selectivity) : selectivity_(selectivity), threshold_(100 * selectivity) {}

  void Execute(DataChunk &input, size_t col_id, DataChunk &result) {
    result.Reset();

    auto &target_col = input.data_[col_id];

    size_t result_count = 0;
    vector<uint32_t> result_vector(kBlockSize);

    spike_.Start();
    for (size_t i = 0; i < input.count_; i++) {
      size_t idx = target_col.selection_vector_[i];
      auto &value = target_col.GetValue(idx);
      if (CheckIfPass(value)) result_vector[result_count++] = i;
    }
    BeeProfiler::Get().InsertStatRecord(evaluate_expression, spike_.Elapsed());

    spike_.Start();
    result.Slice(input, result_vector, result_count);
    BeeProfiler::Get().InsertStatRecord(update_sel_vec, spike_.Elapsed());
  }

  bool CheckIfPass(Attribute &value) const {
    size_t v = std::get<size_t>(value);

    return double(v) / 100 < selectivity_;
  }

 private:
  double selectivity_;
  int threshold_;

  Profiler spike_;
  string update_sel_vec = "[Filter - Update Sel Vector]";
  string evaluate_expression = "[Filter - Evaluate Expression]";
};
}

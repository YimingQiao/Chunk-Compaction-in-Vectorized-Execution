#include <iostream>
#include <random>
#include <cstring>

#include "base.h"
#include "data_collection.h"
#include "profiler.h"
#include "setting.h"
#include "filter_operator.h"

using namespace compaction;

void ParseParameters(int argc, char **argv) {
  if (argc != 1) {
    for (int i = 1; i < argc; i++) {
      std::string arg(argv[i]);

      if (arg == "--cols-num") {
        if (i + 1 < argc) {
          kCols = std::stoi(argv[i + 1]);
          i++;
        }
      } else if (arg == "--selectivity") {
        if (i + 1 < argc) {
          kSelectivity = std::stod(argv[i + 1]);
          i++;
        }
      } else if (arg == "--tuple-size") {
        if (i + 1 < argc) {
          kTupleSize = std::stoi(argv[i + 1]);
          i++;
        }
      } else if (arg == "--filter-num") {
        if (i + 1 < argc) {
          kFilter = std::stoi(argv[i + 1]);
          i++;
        }
      }
    }
  }

  // show the setting
  std::cerr << "------------------ Setting ------------------\n";
  std::cerr << "Selection Vector: Multiple\n"
  std::cerr << "Number of Filters: " << kFilter << "\n"
            << "Number of Columns: " << kCols << "\n"
            << "Filter Selectivity: " << kSelectivity << "\n"
            << "Number of Tuples: " << kTupleSize << "\n";
}

struct PipelineState {
  vector<unique_ptr<DataChunk>> intermediates;
  vector<unique_ptr<FilterOperator>> filters_;

  explicit PipelineState(size_t num_operator) : filters_(num_operator), intermediates(num_operator) {}
};

void ExecutePipeline(DataChunk &input, PipelineState &state, DataCollection &result_table, size_t level) {
  if (input.count_ == 0) return;

  auto &filters = state.filters_;
  auto &intermediates = state.intermediates;

  // The last operator: ResultCollector
  if (level == filters.size()) {
    if (flag_collect_tuples) result_table.AppendChunk(input);
    return;
  }

  auto &result = intermediates[level];
  filters[level]->Execute(input, level, *result);

  ExecutePipeline(*result, state, result_table, level + 1);
}

// example: filter --filter-num 1 --cols-num 100 --selectivity 0.2 --tuple-size 2000000
int main(int argc, char *argv[]) {
  ParseParameters(argc, argv);

  // random generator
  std::random_device rd;
  std::mt19937 gen(2);
  // integer value are in [0, 100), so that I can control the filter selectivity
  std::uniform_int_distribution<> dist(0, 100);

  // ---------------------------------------------- Query Setting ----------------------------------------------

  // create table: (id1, id2, ..., idn, miscellaneous)
  vector<AttributeType> types;
  for (size_t i = 0; i < kCols; ++i) types.push_back(AttributeType::INTEGER);
  types.push_back(AttributeType::STRING);
  compaction::DataCollection table(types);
  vector<compaction::Attribute> tuple(kCols + 1);
  tuple[kCols] = "|";
  for (size_t i = 0; i < kTupleSize; ++i) {
    for (size_t j = 0; j < kCols; ++j) tuple[j] = size_t(dist(gen));
    table.AppendTuple(tuple);
  }

  // create filter operator: selectivity.
  PipelineState state(kFilter);
  auto &filters = state.filters_;
  auto &intermediates = state.intermediates;
  for (size_t i = 0; i < kFilter; ++i) {
    filters[i] = std::make_unique<FilterOperator>(kSelectivity);
    intermediates[i] = std::make_unique<DataChunk>(types);
  }

  // create the result_table collection
  DataCollection result_table(types);

  // -----------------------------------------------------------------------------------------------------------

  double latency = 0;
  Profiler timer;
  {
    // Start process each chunk in the lhs table
    size_t num_chunk_size = kBlockSize;
    size_t start = 0;
    size_t end;
    do {
      end = std::min(start + num_chunk_size, kTupleSize);
      // num_chunk_size = (num_chunk_size + 1) % kBlockSize;
      DataChunk chunk = table.FetchChunk(start, end);
      start = end;

      timer.Start();
      ExecutePipeline(chunk, state, result_table, 0);
      latency += timer.Elapsed();
    } while (end < kTupleSize);
  }

  std::cerr << "------------------ Statistic ------------------\n";
  std::cerr << "[Total Time]: " << latency << "s\n";
  BeeProfiler::Get().EndProfiling();

  if (flag_collect_tuples) {
    // show the joined result.
    std::cout << "Number of tuples in the result table: " << result_table.NumTuples() << "\n";
    result_table.Print(8);
  }

  return 0;
}

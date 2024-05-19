#include <iostream>
#include <random>
#include <cstring>

#include "base.h"
#include "hash_table.h"
#include "data_collection.h"
#include "profiler.h"
#include "compactor.h"
#include "setting.h"
#include "filter_operator.h"

using namespace compaction;

struct PipelineState {
  vector<unique_ptr<FilterOperator>> filters;
  vector<unique_ptr<HashTable>> hts;
  vector<unique_ptr<DataChunk>> intermediates;
  vector<unique_ptr<NaiveCompactor>> compactors;

  explicit PipelineState(size_t n_operator)
      : filters(n_operator), hts(n_operator), intermediates(n_operator), compactors(n_operator) {}
};

static void ExecutePipeline(DataChunk &input, PipelineState &state, DataCollection &result_table, size_t level);

void FlushPipelineCache(PipelineState &state, DataCollection &result_table, size_t level);

std::vector<size_t> ParseList(const std::string &s);

void ParseParameters(int argc, char *argv[]);

// example: compaction --join-num 4 --chunk-factor 5 --lhs-size 20000000 --rhs-size 2000000 --load-factor 0.5 --payload-length=[0,0,0,0]
int main(int argc, char *argv[]) {
  ParseParameters(argc, argv);

  // random generator
  std::random_device rd;
  std::mt19937 gen(2);
  std::uniform_int_distribution<> dist(0, kRHSTupleSize);
  std::uniform_int_distribution<> dist_filter(0, 100);
  size_t n_operator = kJoins + 1;

  // ---------------------------------------------- Query Setting ----------------------------------------------

  // create probe table: (id1, id2, ..., idn, miscellaneous)
  vector<AttributeType> types;
  for (size_t i = 0; i < n_operator; ++i) types.push_back(AttributeType::INTEGER);
  types.push_back(AttributeType::STRING);
  compaction::DataCollection table(types);
  vector<compaction::Attribute> tuple(n_operator + 1);
  tuple[n_operator] = "|";
  for (size_t i = 0; i < kLHSTupleSize; ++i) {
    tuple[0] = size_t(dist_filter(gen));
    for (size_t j = 1; j < n_operator; ++j) tuple[j] = size_t(dist(gen));
    table.AppendTuple(tuple);
  }

  // create rhs hash table, and the result_table chunk
  PipelineState state(n_operator);
  auto &hts = state.hts;
  auto &filters = state.filters;
  auto &intermediates = state.intermediates;
  auto &compactors = state.compactors;

  filters[0] = std::make_unique<FilterOperator>(kSelectivity);
  intermediates[0] = std::make_unique<DataChunk>(types);
  for (size_t i = 1; i < n_operator; ++i) {
    types.push_back(AttributeType::INTEGER);
    types.push_back(AttributeType::STRING);
    intermediates[i] = std::make_unique<DataChunk>(types);
    compactors[i] = std::make_unique<NaiveCompactor>(types);
    hts[i] = std::make_unique<HashTable>(kRHSTupleSize, kChunkFactor, kRHSPayLoadLength[i], types, kLoadFactor);
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
      end = std::min(start + num_chunk_size, kLHSTupleSize);
      // num_chunk_size = (num_chunk_size + 1) % kBlockSize;
      DataChunk chunk = table.FetchChunk(start, end);
      start = end;

      timer.Start();
      ExecutePipeline(chunk, state, result_table, 0);
      latency += timer.Elapsed();
    } while (end < kLHSTupleSize);

#ifdef flag_full_compact
    timer.Start();
    {
      // Flush the tuples in cache.
      FlushPipelineCache(state, result_table, 0);
    }
    latency += timer.Elapsed();
#endif
  }

  std::cerr << "------------------ Statistic ------------------\n";
  std::cerr << "[Total Time]: " << latency << "s\n";
  BeeProfiler::Get().EndProfiling();
  ZebraProfiler::Get().ToCSV();

  if (flag_collect_tuples) {
    // show the joined result.
    std::cout << "Number of tuples in the result table: " << result_table.NumTuples() << "\n";
    result_table.Print(8);
  }

  return 0;
}

void ExecutePipeline(DataChunk &input, PipelineState &state, DataCollection &result_table, size_t level) {
  // The last operator: ResultCollector
  if (level == state.hts.size()) {
    if (flag_collect_tuples) result_table.AppendChunk(input);
    return;
  }

  auto &ht = state.hts[level];
  auto &join_key = input.data_[level];
  auto &result = state.intermediates[level];
  auto &compactor = state.compactors[level];
  auto &filter = state.filters[level];

  if (ht != nullptr) {
    // hash join
    auto ss = ht->Probe(join_key);
    while (ss.HasNext()) {
      ss.Next(join_key, input, *result, kEnableLogicalCompact);

#ifdef flag_full_compact
      // A compactor sits here.
    compactor->Compact(result);
    if (result->count_ == 0) continue;
#endif

      if (result->count_ != 0) ExecutePipeline(*result, state, result_table, level + 1);
    }
  } else if (filter != nullptr) {
    // filter
    filter->Execute(input, level, *result);

#ifdef flag_full_compact
    // A compactor sits here.
    compactor->Compact(result);
    if (result->count_ == 0) continue;
#endif

    if (result->count_ != 0) ExecutePipeline(*result, state, result_table, level + 1);
  }
}

void FlushPipelineCache(PipelineState &state, DataCollection &result_table, size_t level) {
  auto &hts = state.hts;
  auto &intermediates = state.intermediates;
  auto &compactors = state.compactors;

  // The last operator: ResultCollector. It has no compactor.
  if (level == hts.size()) return;

  auto &result = intermediates[level];
  auto &compactor = compactors[level];

  // Fetch the remaining tuples in the cache.
  compactor->Flush(result);

  // Continue the pipeline execution.
  ExecutePipeline(*result, state, result_table, level + 1);

  // Flush the next level.
  FlushPipelineCache(state, result_table, level + 1);
}

void ParseParameters(int argc, char **argv) {
  if (argc != 1) {
    for (int i = 1; i < argc; i++) {
      std::string arg(argv[i]);

      if (arg == "--join-num") {
        if (i + 1 < argc) {
          kJoins = std::stoi(argv[i + 1]);
          i++;
        }
      } else if (arg == "--chunk-factor") {
        if (i + 1 < argc) {
          kChunkFactor = std::stoi(argv[i + 1]);
          i++;
        }
      } else if (arg == "--lhs-size") {
        if (i + 1 < argc) {
          kLHSTupleSize = std::stoi(argv[i + 1]);
          i++;
        }
      } else if (arg == "--rhs-size") {
        if (i + 1 < argc) {
          kRHSTupleSize = std::stoi(argv[i + 1]);
          i++;
        }
      } else if (arg == "--load-factor") {
        if (i + 1 < argc) {
          kLoadFactor = std::stod(argv[i + 1]);
          i++;
        }
      } else if (arg.substr(0, 16) == "--payload-length") {
        // --payload-length=[0,1000,0,0]
        kRHSPayLoadLength = ParseList(arg.substr(17));
      } else if (arg == "--selectivity") {
        if (i + 1 < argc) {
          kSelectivity = std::stod(argv[i + 1]);
        }
      }
    }

    if (kJoins != kRHSPayLoadLength.size())
      throw std::runtime_error("Payload vector length must equal to the number of joins.");
  }

  // show the setting
  std::cerr << "------------------ Setting ------------------\n";
  if (kEnableLogicalCompact) std::cerr << "Strategy: logical_compaction\n";
  else std::cerr << "Compaction Strategy: no_compaction\n";
  std::cerr << "Number of Joins: " << kJoins << "\n"
            << "Number of LHS Tuple: " << kLHSTupleSize << "\n"
            << "Number of RHS Tuple: " << kRHSTupleSize << "\n"
            << "Chunk Factor: " << kChunkFactor << "\n"
            << "Load Factor: " << kLoadFactor << "\n"
            << "Filter Selectivity: " << kSelectivity << "\n";
  std::cerr << "RHS Payload Lengths: [";
  for (size_t i = 0; i < kJoins; ++i) {
    if (i != kJoins - 1) std::cerr << kRHSPayLoadLength[i] << ",";
    else std::cerr << kRHSPayLoadLength[i] << "]\n";
  }
}

std::vector<size_t> ParseList(const string &s) {
  std::stringstream ss(s.substr(1, s.size() - 2)); // Ignore brackets
  std::vector<size_t> result;
  std::string item;
  while (std::getline(ss, item, ',')) {
    result.push_back(std::stoi(item));
  }
  return result;
}

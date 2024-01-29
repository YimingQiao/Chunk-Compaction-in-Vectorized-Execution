#include <iostream>
#include <random>

#include "base.h"
#include "hash_table.h"
#include "data_collection.h"
#include "profiler.h"
#include "compactor.h"

using namespace compaction;

const size_t kJoins = 3;
const size_t kLHSTupleSize = 2e7;
const size_t kRHSTupleSize = 2e6;
const size_t kChunkFactor = 8;

#define BINARY_COMPACT

#ifdef FULL_COMPACT
using Compactor = NaiveCompactor;
#elif defined(BINARY_COMPACT)
using Compactor = BinaryCompactor;
#elif defined(DYNAMIC_COMPACT)
using Compactor = DynamicCompactor;
#else
using Compactor = NaiveCompactor;
#endif

struct PipelineState {
  vector<unique_ptr<HashTable>> hts;
  vector<unique_ptr<DataChunk>> intermediates;
  vector<unique_ptr<Compactor>> compactors;

  PipelineState() : hts(kJoins), intermediates(kJoins), compactors(kJoins) {}
};

static void ExecutePipeline(DataChunk &input, PipelineState &state, DataCollection &result_table, size_t level) {
  auto &hts = state.hts;
  auto &intermediates = state.intermediates;
  auto &compactors = state.compactors;

  // The last operator: ResultCollector
  if (level == hts.size()) {
    result_table.AppendChunk(input);
    return;
  }

  auto &join_key = input.data_[level];
  auto &result = intermediates[level];
  auto &compactor = compactors[level];

#ifdef DYNAMIC_COMPACT
  // ---------------------------------- learn compaction thresholds -------------------------------------
  Profiler profiler;
  profiler.Start();
  // 1. Get a compaction threshold
  // 2. Start to record execution time
  size_t threshold = CompactTuner::Get().SelectArm(level);
  compactor->SetThreshold(threshold);

  BeeProfiler::Get().InsertStatRecord("[UCB Get Thresholds]", profiler.Elapsed());

  profiler.Start();
  // -----------------------------------------------------------------------------------------------------
#endif

  auto ss = hts[level]->Probe(join_key, input.count_, input.selection_vector_);
  while (ss.HasNext()) {
    ss.Next(join_key, input, *result);

#if defined(FULL_COMPACT) || defined(BINARY_COMPACT) || defined(DYNAMIC_COMPACT)
    // A compactor sits here.
    compactor->Compact(result);
    if (result->count_ == 0) continue;
#endif

    ExecutePipeline(*result, state, result_table, level + 1);
  }

#ifdef DYNAMIC_COMPACT
  // ---------------------------------- learn compaction thresholds -------------------------------------
  double time = profiler.Elapsed();
  profiler.Start();
  CompactTuner::Get().UpdateArm(level, compactor->GetThreshold(), 1 / time / 1e2);
  BeeProfiler::Get().InsertStatRecord("[UCB Update]", profiler.Elapsed());
  // -----------------------------------------------------------------------------------------------------
#endif
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

int main() {
  // random generator
  std::random_device rd;
  std::mt19937 gen(2);
  std::uniform_int_distribution<> dist(0, kRHSTupleSize);

  // ---------------------------------------------- Query Setting ----------------------------------------------

  // create probe table: (id1, id2, ..., idn, miscellaneous)
  vector<AttributeType> types;
  for (size_t i = 0; i < kJoins; ++i) types.push_back(AttributeType::INTEGER);
  types.push_back(AttributeType::STRING);
  compaction::DataCollection table(types);
  vector<compaction::Attribute> tuple(kJoins + 1);
  tuple[kJoins] = "|";
  for (size_t i = 0; i < kLHSTupleSize; ++i) {
    for (size_t j = 0; j < kJoins; ++j) tuple[j] = size_t(dist(gen));
    table.AppendTuple(tuple);
  }

  // create rhs hash table, and the result_table chunk
  PipelineState state;
  auto &hts = state.hts;
  auto &intermediates = state.intermediates;
  auto &compactors = state.compactors;
  for (size_t i = 0; i < kJoins; ++i) {
    hts[i] = std::make_unique<HashTable>(kRHSTupleSize, kChunkFactor);
    types.push_back(AttributeType::INTEGER);
    types.push_back(AttributeType::STRING);
    intermediates[i] = std::make_unique<DataChunk>(types);
    // compactors[i] = std::make_unique<NaiveCompactor>(types);
    compactors[i] = std::make_unique<Compactor>(types);
  }

  // create the result_table collection
  DataCollection result_table(types);

#ifdef DYNAMIC_COMPACT
  // create MAB for each join operator
  for (size_t i = 0; i < kJoins; ++i) CompactTuner::Get().Initialize(size_t(&state.compactors[i]));
#endif
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

#if defined(FULL_COMPACT) || defined(BINARY_COMPACT) || defined(DYNAMIC_COMPACT)
    timer.Start();
    {
      // Flush the tuples in cache.
      FlushPipelineCache(state, result_table, 0);
    }
    latency += timer.Elapsed();
#endif

    std::cout << "[Total Time]: " << latency << "s\n";
  }

  BeeProfiler::Get().EndProfiling();
  // ZebraProfiler::Get().PrintResults();
  ZebraProfiler::Get().ToCSV();
  CompactTuner::Get().Reset();

  // show the joined result.
  std::cout << "Number of tuples in the result table: " << result_table.NumTuples() << "\n";
  result_table.Print(8);

  return 0;
}


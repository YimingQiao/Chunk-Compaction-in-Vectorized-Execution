#include <iostream>
#include <random>

#include "base.h"
#include "hash_table.h"
#include "data_collection.h"
#include "profiler.h"
#include "compactor.h"

//#define COMPACT

using namespace compaction;

const size_t kJoins = 3;
const size_t kLHSTupleSize = 1e7;
const size_t kRHSTupleSize = 1e6;
const size_t kChunkFactor = 8;

struct PipelineState {
  vector<unique_ptr<HashTable>> hts;
  vector<unique_ptr<DataChunk>> intermediates;
  vector<unique_ptr<NaiveCompactor>> compactors;

  PipelineState() : hts(kJoins), intermediates(kJoins), compactors(kJoins) {}
};

static void ExecutePipeline(DataChunk &input, PipelineState &state, DataCollection &result_table, size_t level) {
  auto &hts = state.hts;
  auto &intermediates = state.intermediates;
  auto &compactors = state.compactors;

  // The last operator: ResultCollector
  if (level == hts.size()) {
    // result_table.AppendChunk(input);
    return;
  }

  auto &join_key = input.data_[level];
  auto &result = intermediates[level];
  auto &compactor = compactors[level];

  auto ss = hts[level]->Probe(join_key);
  while (ss.HasNext()) {
    ss.Next(join_key, input, *result);

#ifdef COMPACT
    // A compactor sits here.
    compactor->Compact(result);
    if (result->count_ == 0) continue;
#endif

    ExecutePipeline(*result, state, result_table, level + 1);
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

int main() {
  // random generator
  std::random_device rd;
  std::mt19937 gen(0);
  std::uniform_int_distribution<> dist(0, kRHSTupleSize);

  // create probe table: (id, course_id, major_id, miscellaneous)
  vector<AttributeType> types
      {AttributeType::INTEGER, AttributeType::INTEGER, AttributeType::INTEGER, AttributeType::STRING};
  compaction::DataCollection table(types);
  for (size_t i = 0; i < kLHSTupleSize; ++i) {
    std::vector<compaction::Attribute> tuple{size_t(dist(gen)), size_t(dist(gen)), size_t(dist(gen)), "|"};
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
    compactors[i] = std::make_unique<NaiveCompactor>(types);
  }

  // create the result_table collection
  DataCollection result_table(types);

  {
    Profiler profiler;
    profiler.Start();

    // Start process each chunk in the lhs table
    size_t num_chunk_size = kBlockSize;
    size_t start = 0;
    size_t end;
    do {
      end = std::min(start + num_chunk_size, kLHSTupleSize);
      num_chunk_size = (num_chunk_size + 1) % kBlockSize;
      DataChunk chunk = table.FetchChunk(start, end);
      start = end;

      ExecutePipeline(chunk, state, result_table, 0);
    } while (end < kLHSTupleSize);

#ifdef COMPACT
    // Flush the tuples in cache.
    FlushPipelineCache(state, result_table, 0);
#endif

    double latency = profiler.Elapsed();
    std::cout << "[Total Time]: " << latency << "s\n";
  }

  BeeProfiler::Get().EndProfiling();
  ZebraProfiler::Get().ToCSV();

  // show the joined result.
  std::cout << "Number of tuples in the result table: " << result_table.NumTuples() << "\n";
  result_table.Print(10);

  return 0;
}


#include <iostream>
#include <random>

#include "base.h"
#include "hash_table.h"
#include "data_collection.h"
#include "profiler.h"

using namespace compaction;

const size_t kLHSTupleSize = 5e6;
const size_t kRHSTupleSize = 5e5;
const size_t kChunkFactor = 1;

int main() {
  // random generator
  std::random_device rd;
  std::mt19937 gen(rd());
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
  HashTable ht_1(kRHSTupleSize, kChunkFactor);
  types.push_back(AttributeType::INTEGER);
  types.push_back(AttributeType::STRING);
  DataChunk result_chunk_1(types);

  HashTable ht_2(kRHSTupleSize, kChunkFactor);
  types.push_back(AttributeType::INTEGER);
  types.push_back(AttributeType::STRING);
  DataChunk result_chunk_2(types);

  HashTable ht_3(kRHSTupleSize, kChunkFactor);
  types.push_back(AttributeType::INTEGER);
  types.push_back(AttributeType::STRING);
  DataChunk result_chunk_3(types);

  // create the result_table collection
  DataCollection result_table(types);

  {
    Profiler profiler, profiler_join_1, profiler_join_2, profiler_join_3;
    profiler.Start();

    // Start process each chunk in the lhs table
    size_t start = 0;
    size_t end;
    do {
      end = std::min(start + kBlockSize, kLHSTupleSize);
      DataChunk chunk = table.FetchChunk(start, end);
      start = end;

      profiler_join_1.Start();

      // Join: 1
      Vector &join_key = chunk.data_[0];
      auto ss = ht_1.Probe(join_key);

      while (ss.HasNext()) {
        ss.Next(join_key, chunk, result_chunk_1);

        profiler_join_2.Start();

        // Join: 2
        Vector &join_key = result_chunk_1.data_[1];
        auto ss = ht_2.Probe(join_key);

        while (ss.HasNext()) {
          ss.Next(join_key, result_chunk_1, result_chunk_2);

          profiler_join_3.Start();

          // Join: 3
          Vector &join_key = result_chunk_2.data_[2];
          auto ss = ht_3.Probe(join_key);

          while (ss.HasNext()) {
            ss.Next(join_key, result_chunk_2, result_chunk_3);
            // result_table.AppendChunk(result_chunk_3);
          }

          double time = profiler_join_3.Elapsed();
          ZebraProfiler::Get().InsertRecord("3rd_join_IN", result_chunk_2.count_, time);
        }

        double time = profiler_join_2.Elapsed();
        ZebraProfiler::Get().InsertRecord("2nd_join_IN", result_chunk_1.count_, time);
      }

      double time = profiler_join_1.Elapsed();
      ZebraProfiler::Get().InsertRecord("1st_join_IN", chunk.count_, time);
    } while (end < kLHSTupleSize);

    double latency = profiler.Elapsed();
    std::cout << "[Total Time]: " << latency << "s\n";
  }

  ZebraProfiler::Get().ToCSV();

  // show the joined result.
//  std::cout << "Number of tuples in the result table: " << result_table.NumTuples() << "\n";
//  result_table.Print(10);

  return 0;
}


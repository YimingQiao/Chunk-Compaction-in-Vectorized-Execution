#include <iostream>
#include <random>

#include "base.h"
#include "hash_table.h"
#include "data_collection.h"

using namespace compaction;

const size_t kLHSTupleSize = 8;
const size_t kRHSTupleSize = 8;
const size_t kChunkFactor = 2;

int main() {
  // random generator
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> age_dist(18, 100);
  std::uniform_int_distribution<> course_id_dist(0, kRHSTupleSize);
  std::uniform_int_distribution<> major_id_dist(0, kRHSTupleSize);

  // create probe table: (id, course_id, major_id, age, name)
  vector<AttributeType> types
      {AttributeType::INTEGER, AttributeType::INTEGER, AttributeType::INTEGER,
       AttributeType::INTEGER, AttributeType::STRING};
  compaction::DataCollection table(types);
  for (size_t i = 0; i < kLHSTupleSize; ++i) {
    std::vector<compaction::Attribute> tuple{i, size_t(course_id_dist(gen)), size_t(major_id_dist(gen)),
                                             size_t(age_dist(gen)), "name_" + std::to_string(i)};
    table.AppendTuple(tuple);
  }

  // create rhs hash table, and the result_table chunk
  HashTable ht_1(kRHSTupleSize, kChunkFactor);
  types.push_back(AttributeType::STRING);
  DataChunk result_chunk_1(types);

  HashTable ht_2(kRHSTupleSize, kChunkFactor);
  types.push_back(AttributeType::STRING);
  DataChunk result_chunk_2(types);

  HashTable ht_3(kRHSTupleSize, kChunkFactor);
  types.push_back(AttributeType::STRING);
  DataChunk result_chunk_3(types);

  // create the result_table collection
  DataCollection result_table(types);

  // Start process each chunk in the lhs table
  size_t start = 0;
  size_t end;
  do {
    end = std::min(start + kBlockSize, kLHSTupleSize);
    DataChunk chunk = table.FetchChunk(start, end);
    start = end;

    // start probe
    Vector &join_key = chunk.data_[0];
    auto ss = ht_1.Probe(join_key);

    while (ss.HasNext()) {
      ss.Next(join_key, chunk, result_chunk_1);

      Vector &join_key = result_chunk_1.data_[1];
      auto ss = ht_2.Probe(join_key);

      while (ss.HasNext()) {
        ss.Next(join_key, result_chunk_1, result_chunk_2);

        Vector &join_key = result_chunk_2.data_[2];
        auto ss = ht_3.Probe(join_key);

        while (ss.HasNext()) {
          ss.Next(join_key, result_chunk_2, result_chunk_3);
          result_table.AppendChunk(result_chunk_3);
        }
      }
    }
  } while (end < kLHSTupleSize);

  // show the joined result.
  result_table.Print();

  return 0;
}


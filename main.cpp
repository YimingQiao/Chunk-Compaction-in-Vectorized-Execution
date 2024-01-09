#include <iostream>
#include <random>

#include "base.h"
#include "hash_table.h"

int main() {
  size_t n_tuple = 50;
  size_t n_column = 3;
  std::vector<std::vector<compaction::Attribute>> table(n_column);

  // random generator
  std::random_device rd;
  std::mt19937 gen(rd());

  // Column 1: stu_id, integer
  auto &stu_id = table[0];
  stu_id.resize(n_tuple);
  for (size_t i = 0; i < n_tuple; ++i) stu_id[i] = i;

  // Column 2: age, integer
  auto &age = table[1];
  age.resize(n_tuple);
  std::uniform_int_distribution<> dis(18, 100);
  for (size_t i = 0; i < n_tuple; ++i) age[i] = size_t(dis(gen));

  // Column 3: name, string
  auto &name = table[2];
  name.resize(n_tuple);
  std::uniform_int_distribution<> dis_name(0, 1000000);
  for (size_t i = 0; i < n_tuple; ++i) name[i] = "My Name is " + std::to_string(dis_name(gen));

  std::vector<compaction::AttributeType>
      types{compaction::AttributeType::INTEGER, compaction::AttributeType::INTEGER, compaction::AttributeType::STRING};
  compaction::DataChunk chunk(types);

  size_t size_chunk = std::min(compaction::kBlockSize, table[0].size());
  for (size_t i = 0; i < n_column; ++i) {
    auto &col = chunk.data_[i];
    for (size_t j = 0; j < size_chunk; ++j) {
      col.GetValue(j) = table[i][j];
    }
    col.count_ = size_chunk;
  }
  chunk.count_ = size_chunk;

  types.push_back(compaction::AttributeType::INTEGER);
  compaction::DataChunk result(types);

  compaction::HashTable hash_table(2048, 4);
  compaction::Vector &join_key = chunk.data_[0];
  auto ss = hash_table.Probe(join_key);

  while (ss.HasNext()) {
    ss.Next(join_key, chunk, result);
    result.Print();
  }
}


//===----------------------------------------------------------------------===//
//
//                         Compaction
//
// hash_table
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <unordered_map>

#include "base.h"

namespace compaction {
//! JoinHashTable is a linear probing HT that is used for computing joins
/*!
   The JoinHashTable concatenates incoming chunks inside a linked list of
   data ptrs. The storage looks like this internally.
   [SERIALIZED ROW][NEXT POINTER]
   [SERIALIZED ROW][NEXT POINTER]
   There is a separate hash map of pointers that point into this table.
   This is what is used to resolve the hashes.
   [POINTER]
   [POINTER]
   [POINTER]
   The pointers are either NULL
*/
class HashTable {
 public:
  HashTable(size_t n_bucket, size_t n_tuples) {
    linked_lists_.resize(n_bucket);
    // TODO: create a hash table, that has two functions: (1) Probe; (2) Next.
    //  The probe is to find the bucket address for a chunk; while the Next is called to return a result data chunk.
  }

 private:
  vector<list<Tuple>> linked_lists_;
  unordered_map<Attribute, list<Tuple>> hash_map_;
};
}

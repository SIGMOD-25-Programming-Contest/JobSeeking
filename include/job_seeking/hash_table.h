#ifndef JOB_SEEKING_HASH_TABLE_H_
#define JOB_SEEKING_HASH_TABLE_H_

#include <bit>

#include "allocator.h"
#include "utils.h"

namespace job_seeking {

// a sequential hash table
template <typename Key, typename Value>
struct my_hash_table {
  int capacity, cnt;
  std::pair<Key, Value>* table;
  char* used;

  my_hash_table() : capacity(0), cnt(0) {}

  void reserve(int n) {
    capacity = 64;
    while (capacity < 4 * n) capacity *= 2;
    table = (std::pair<Key, Value>*)alloc::string_allocator->malloc(
        capacity * sizeof(std::pair<Key, Value>));
    used = (char*)alloc::data_allocator->malloc(capacity * sizeof(char));
  }

  void insert(const Key& key, const Value& value) {
    cnt++;
    size_t index = my_hash_2(key) % capacity;
    for (size_t i = 0; i < capacity; ++i) {
      size_t probe_index = (index + i) % capacity;
      if (!used[probe_index]) {
        table[probe_index] = {key, value};
        used[probe_index] = 1;
        return;
      }
    }
    exit(1);
  }

  std::optional<Value> find(const Key& key) const {
    if (cnt == 0) return std::nullopt;
    size_t index = my_hash_2(key) % capacity;
    for (size_t i = 0; i < capacity; ++i) {
      size_t probe_index = (index + i) % capacity;
      if (!used[probe_index]) return std::nullopt;
      if (table[probe_index].first == key) {
        return table[probe_index].second;
      }
    }
    return std::nullopt;
  }
};

}  // namespace job_seeking

#endif  // JOB_SEEKING_HASH_TABLE_H_

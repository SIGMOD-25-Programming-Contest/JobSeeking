#ifndef JOB_SEEKING_SORT_H_
#define JOB_SEEKING_SORT_H_

#include <functional>
#include <optional>

#include "allocator.h"
#include "debug.h"
#include "hash_table.h"
#include "my_scheduler.h"
#include "utils.h"

namespace job_seeking {

// integer sort

const int kIntegerSortBaseCase = 30000;
const int kIntegerSortSampleFactor = 50;
const int kIntegerSortNumLightBuckets = 128;
const int kIntegerSortBlockSize = 10000;

struct IntegerSort {
  std::pair<int, int>* items;
  int n;
  my_hash_table<int, std::pair<int, int>>* heavy_hash_map;
  std::vector<my_hash_table<int, std::pair<int, int>>*> light_hash_maps;

  std::function<std::optional<std::pair<int, int>>(int)> get_interval;

  IntegerSort(std::pair<int, int>* items, int n) : items(items), n(n) {
    heavy_hash_map = new my_hash_table<int, std::pair<int, int>>;
  }

  void Work(int l, int r, my_hash_table<int, std::pair<int, int>>& hash_map) {
    if (l >= r) return;
    std::sort(items + l, items + r);
    for (int x = l, y; x < r; x = y) {
      y = x + 1;
      if (items[x].first == 0) continue;
      while (y < r && items[y].first == items[x].first) y++;
      hash_map.insert(items[x].first, {x, y});
    }
  }

  void Sort() {
    // debug("Sort", items);
    if (n == 0) {
      get_interval = [&](int key) -> std::optional<std::pair<int, int>> {
        return std::nullopt;
      };
      return;
    }
    if (n < kIntegerSortBaseCase) {
      heavy_hash_map->reserve(n);
      Work(0, n, *heavy_hash_map);
      get_interval = [&](int key) -> std::optional<std::pair<int, int>> {
        return heavy_hash_map->find(key);
      };
      return;
    }

    // timer t;

    int num_samples = n / kIntegerSortSampleFactor + 1;
    for (int i = 0; i < num_samples; i++) {
      int idx = my_hash((uint32_t)i) % n;
      std::swap(items[i], items[idx]);
    }
    std::sort(items, items + num_samples);
    // t.next("get samples and sort");

    std::unordered_map<int, int> cnt;
    for (int i = 0; i < num_samples; i++) {
      if (items[i].first == 0) continue;
      cnt[items[i].first]++;
    }
    // t.next("get cnt");

    int threshold = num_samples / 256 + 1;

    std::unordered_map<int, int> heavy_bucket_id;
    int heavy_cnt = 0;
    for (int i = 0; i < num_samples; i++) {
      if (items[i].first == 0) continue;
      if (cnt[items[i].first] >= threshold &&
          heavy_bucket_id.find(items[i].first) == heavy_bucket_id.end()) {
        heavy_bucket_id[items[i].first] = heavy_cnt++;
      }
    }
    // t.next("get heavy_bucket_id");

    auto get_bucket_id = [&](int key) {
      auto it = heavy_bucket_id.find(key);
      if (it != heavy_bucket_id.end()) {
        return it->second;
      } else {
        return heavy_cnt + int(my_hash(key) % kIntegerSortNumLightBuckets);
      }
    };

    int num_buckets = heavy_cnt + kIntegerSortNumLightBuckets;
    int num_blocks = (n + kIntegerSortBlockSize - 1) / kIntegerSortBlockSize;

    std::vector<std::vector<int>> block_bucket(
        num_blocks + 1, std::vector<int>(num_buckets, 0));
    // t.next("allocate block_bucket");

    job_seeking_scheduler.parfor(0, num_blocks, [&](int block_id) {
      int l = block_id * kIntegerSortBlockSize;
      int r = std::min(n, (block_id + 1) * kIntegerSortBlockSize);
      for (int i = l; i < r; i++) {
        if (items[i].first == 0) continue;
        block_bucket[block_id + 1][get_bucket_id(items[i].first)]++;
      }
    });
    // t.next("count block_bucket");

    for (int i = 0; i < num_blocks; i++) {
      for (int j = 0; j < num_buckets; j++) {
        block_bucket[i + 1][j] += block_bucket[i][j];
      }
    }
    // t.next("prefix sum block_bucket");

    // int max_light_bucket_size = 0;
    // for (int i = 0; i < num_buckets; i++) {
    //   max_light_bucket_size =
    //       std::max(max_light_bucket_size, block_bucket[num_blocks][i]);
    // }
    // t.next("compute max_light_bucket_size");

#if defined(DEBUG_OUTPUT)
    debug(num_samples, threshold, heavy_cnt, num_buckets, num_blocks);
#endif  // DEBUG_OUTPUT

    std::vector<int> bucket_offset(num_buckets + 1);
    for (int i = 0; i < num_buckets; i++) {
      bucket_offset[i + 1] = bucket_offset[i] + block_bucket[num_blocks][i];
    }
    // t.next("compute bucket_offset");

    std::pair<int, int>* tmp =
        (std::pair<int, int>*)alloc::string_allocator->malloc(
            sizeof(std::pair<int, int>) * n);
    job_seeking_scheduler.parfor(0, num_blocks, [&](int block_id) {
      int l = block_id * kIntegerSortBlockSize;
      int r = std::min(n, (block_id + 1) * kIntegerSortBlockSize);
      for (int i = l; i < r; i++) {
        if (items[i].first == 0) continue;
        int bucket_id = get_bucket_id(items[i].first);
        int pos =
            bucket_offset[bucket_id] + block_bucket[block_id][bucket_id]++;
        tmp[pos] = items[i];
      }
    });
    std::swap(items, tmp);
    // items.resize(bucket_offset[num_buckets]);
    // t.next("rearrange items");

    heavy_hash_map->reserve(heavy_cnt);
    for (int i = 0; i < heavy_cnt; i++) {
      int l = bucket_offset[i], r = bucket_offset[i + 1];
      heavy_hash_map->insert(items[l].first, {l, r});
    }
    // t.next("build heavy hash table");

    light_hash_maps.resize(kIntegerSortNumLightBuckets);
    for (int i = heavy_cnt; i < num_buckets; i++) {
      light_hash_maps[i - heavy_cnt] =
          new my_hash_table<int, std::pair<int, int>>;
      light_hash_maps[i - heavy_cnt]->reserve(bucket_offset[i + 1] -
                                              bucket_offset[i]);
    }
    // t.next("build light hash tables");

    job_seeking_scheduler.parfor(heavy_cnt, num_buckets, [&](int i) {
      int l = bucket_offset[i], r = bucket_offset[i + 1];
      Work(l, r, *light_hash_maps[i - heavy_cnt]);
    });

    // t.next("sort light buckets");

    get_interval = [&](int key) -> std::optional<std::pair<int, int>> {
      if (key == 0) return std::nullopt;
      auto it = heavy_hash_map->find(key);
      if (it.has_value()) return it.value();
      int bucket_id =
          heavy_cnt + int(my_hash(key) % kIntegerSortNumLightBuckets);
      auto& light_map = light_hash_maps[bucket_id - heavy_cnt];
      return light_map->find(key);
    };
  }
};

}  // namespace job_seeking

#endif  // JOB_SEEKING_SORT_H_

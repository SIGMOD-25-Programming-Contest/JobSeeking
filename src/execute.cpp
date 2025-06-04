#include <hardware.h>
#include <plan.h>
#include <table.h>

#include "common.h"
#include "job_seeking/allocator.h"
#include "job_seeking/debug.h"
#include "job_seeking/my_scheduler.h"
#include "job_seeking/scan.h"
#include "job_seeking/sort.h"
#include "unordered_map"

#define JOB_SEEKING
// #define DEBUG_OUTPUT

template <>
struct DebugPrinter<DataType> {
  DataType x;
  operator std::string() {
    switch (x) {
      case DataType::INT32:
        return "INT32";
      case DataType::INT64:
        return "INT64";
      case DataType::FP64:
        return "FP64";
      case DataType::VARCHAR:
        return "VARCHAR";
      default:
        return "UNKNOWN";
    }
  }
};

namespace job_seeking {

size_t test_cnt = 0;
size_t column_cache_hit_cnt = 0;

std::unordered_map<uint64_t, void**> column_cache;

std::unordered_map<uint64_t, IntegerSort*> hash_table_cache;

struct JobSeekingContext {
  JobSeekingContext() {
    timer t_sche;
    my_scheduler =
        new (scheduler_buf) work_stealing_scheduler(init_num_workers());
    t_sche.next("scheduler");

    scanner_1 = new scanner();
    scanner_2 = new scanner();

    column_cache.reserve(1000);

    hash_table_cache.reserve(1000);

    alloc::data_allocator = new alloc::my_allocator<true>(4000000000ull);
    alloc::string_allocator = new alloc::my_allocator<false>(6000000000ull);
    alloc::general_allocator = new alloc::my_allocator<false>(2000000000ull);
  }

  ~JobSeekingContext() {
    my_scheduler->clear();

    lndebug(column_cache_hit_cnt);
    debug(alloc::data_allocator->used_size, alloc::data_allocator->alloc_cnt);
    debug(alloc::string_allocator->used_size,
          alloc::string_allocator->alloc_cnt);
    debug(alloc::general_allocator->max_history_size,
          alloc::general_allocator->alloc_cnt);

    column_cache.clear();
    hash_table_cache.clear();
    delete scanner_1;
    delete scanner_2;
    for (char* buffer : alloc::data_allocator->all_buffers) delete[] buffer;
    for (char* buffer : alloc::string_allocator->all_buffers) delete[] buffer;
    for (char* buffer : alloc::general_allocator->all_buffers) delete[] buffer;
  }
};

JobSeekingContext* job_seeking_context;

uint64_t Hash(size_t num_rows, const std::vector<Page*>& pages) {
  uint64_t hash = num_rows;
  for (int i = 0; i < 10 && i < (int)pages.size(); i++) {
    auto t = *reinterpret_cast<uint64_t*>(pages[i]);
    detail::hash_combine_impl(hash, t);
  }
  detail::hash_combine_impl(hash, pages.size());
  return hash;
}

static_assert(sizeof(double) == sizeof(void*));

template <typename T>
T GetData(void* data) {
  if (data == nullptr) exit(1);
  if constexpr (std::is_same_v<T, int32_t>) {
    return (int32_t)((int64_t)data);
  } else if constexpr (std::is_same_v<T, int64_t>) {
    return (int64_t)data;
  } else if constexpr (std::is_same_v<T, double>) {
    double x;
    memcpy(&x, &data, sizeof(double));
    return x;
  } else if constexpr (std::is_same_v<T, std::string_view>) {
    return *(std::string_view*)data;
  } else {
    throw std::runtime_error("unknown data type");
  }
}

bool get_bitmap(const uint8_t* bitmap, uint16_t idx) {
  auto byte_idx = idx >> 3;
  auto bit = idx & 7;
  return bitmap[byte_idx] & (1u << bit);
}

template <typename T>
void** read_column(int num_rows, const std::vector<Page*>& pages) {
  uint64_t hash;
  if (num_rows >= 1000) {
    hash = Hash(num_rows, pages);
    auto it = column_cache.find(hash);
    if (it != column_cache.end()) {
      column_cache_hit_cnt++;
      // debug(column_cache_hit_cnt);
      return it->second;
    }
  }
  // timer t;
  void** data = (void**)alloc::data_allocator->malloc(8 * num_rows);
  // t.next("init");
  if constexpr (std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
                std::is_same_v<T, double>) {
    auto& prefix_sum = *scanner_1;
    job_seeking_scheduler.parfor(0, pages.size(), [&](int page_id) {
      auto* page = pages[page_id]->data;
      auto num_rows = *reinterpret_cast<uint16_t*>(page);
      prefix_sum[page_id] = num_rows;
    });
    prefix_sum.scan(pages.size());
    // t.next("scan");
    job_seeking_scheduler.parfor(0, pages.size(), [&](int page_id) {
      auto* page = pages[page_id]->data;
      auto num_rows = *reinterpret_cast<uint16_t*>(page);
      auto* data_begin = reinterpret_cast<T*>(page + 4);
      auto* bitmap =
          reinterpret_cast<uint8_t*>(page + PAGE_SIZE - (num_rows + 7) / 8);
      uint16_t data_idx = 0;
      int pos = prefix_sum[page_id];
      for (uint16_t i = 0; i < num_rows; ++i) {
        if (get_bitmap(bitmap, i)) {
          T value = data_begin[data_idx++];
          void* x;
          if constexpr (std::is_same_v<T, int32_t> ||
                        std::is_same_v<T, int64_t>) {
            x = (void*)((int64_t)value);
          } else {
            memcpy(&x, &value, sizeof(T));
          }
          data[pos + i] = x;
        }
      }
    });
    // t.next("fill");
  } else {  // read strings
    auto& prefix_sum = *scanner_1;
    job_seeking_scheduler.parfor(0, pages.size(), [&](int page_id) {
      auto* page = pages[page_id]->data;
      auto num_rows = *reinterpret_cast<uint16_t*>(page);
      if (num_rows == 0xffff) {
        prefix_sum[page_id] = 1;
      } else if (num_rows == 0xfffe) {
        prefix_sum[page_id] = 0;
      } else {
        prefix_sum[page_id] = num_rows;
      }
    });
    prefix_sum.scan(pages.size());
    char* string_view_output = (char*)alloc::string_allocator->malloc(
        sizeof(std::string_view) * num_rows);

    auto& char_sum = *scanner_2;
    job_seeking_scheduler.parfor(0, pages.size(), [&](int page_id) {
      auto* page = pages[page_id]->data;
      auto num_rows = *reinterpret_cast<uint16_t*>(page);
      if (num_rows == 0xffff || num_rows == 0xfffe) {
        auto num_chars = *reinterpret_cast<uint16_t*>(page + 2);
        char_sum[page_id] = num_chars;
      } else {
        uint16_t num_non_null = *reinterpret_cast<uint16_t*>(page + 2);
        uint16_t* offset_begin = reinterpret_cast<uint16_t*>(page + 4);
        uint16_t last_offset = offset_begin[num_non_null - 1];
        char_sum[page_id] = last_offset;
      }
    });
    int tot = char_sum.scan(pages.size());
    char* char_output = (char*)alloc::string_allocator->malloc(tot);

    job_seeking_scheduler.parfor(0, pages.size(), [&](int page_id) {
      int row_id_start = prefix_sum[page_id];
      char* output_ptr =
          string_view_output + sizeof(std::string_view) * row_id_start;
      char* output_ch = char_output + char_sum[page_id];

      auto* page = pages[page_id]->data;
      auto num_rows = *reinterpret_cast<uint16_t*>(page);
      if (num_rows == 0xffff) {
        auto num_chars = *reinterpret_cast<uint16_t*>(page + 2);
        auto* data_begin = reinterpret_cast<char*>(page + 4);
        int cnt = 0;
        memcpy(output_ch, data_begin, num_chars);
        cnt += num_chars;
        while (page_id + 1 < pages.size()) {
          page_id++;
          auto* page = pages[page_id]->data;
          num_rows = *reinterpret_cast<uint16_t*>(page);
          if (num_rows != 0xfffe) break;
          auto num_chars = *reinterpret_cast<uint16_t*>(page + 2);
          auto* data_begin = reinterpret_cast<char*>(page + 4);
          memcpy(output_ch + cnt, data_begin, num_chars);
          cnt += num_chars;
        }
        data[row_id_start] = new (output_ptr) std::string_view(output_ch, cnt);
      } else if (num_rows == 0xfffe) {
      } else {
        auto num_non_null = *reinterpret_cast<uint16_t*>(page + 2);
        auto* offset_begin = reinterpret_cast<uint16_t*>(page + 4);
        auto* data_begin = reinterpret_cast<char*>(page + 4 + num_non_null * 2);
        auto* string_begin = data_begin;
        auto* bitmap =
            reinterpret_cast<uint8_t*>(page + PAGE_SIZE - (num_rows + 7) / 8);
        uint16_t data_idx = 0;
        uint16_t data_len = 0;
        for (uint16_t i = 0; i < num_rows; ++i) {
          if (get_bitmap(bitmap, i)) {
            auto offset = offset_begin[data_idx++];
            auto len = data_begin + offset - string_begin;
            memcpy(output_ch, string_begin, len);
            data[row_id_start + i] =
                new (output_ptr) std::string_view(output_ch, len);
            string_begin += len;
            output_ptr += sizeof(std::string_view);
            output_ch += len;
          }
        }
      }
    });
  }
  if (num_rows >= 1000) {
    column_cache[hash] = data;
  }
  return data;
}

void** read_column(DataType type, int num_rows,
                   const std::vector<Page*>& pages) {
  switch (type) {
    case DataType::INT32:
      return read_column<int32_t>(num_rows, pages);
    case DataType::INT64:
      return read_column<int64_t>(num_rows, pages);
    case DataType::FP64:
      return read_column<double>(num_rows, pages);
    case DataType::VARCHAR:
      return read_column<std::string_view>(num_rows, pages);
    default:
      throw std::runtime_error("unknown data type");
  }
}

struct ExecuteResult {
  int n_col, n_row;
  std::vector<void**> data;

  ExecuteResult() : n_col(0), n_row(0) {}

  ExecuteResult(int n_col, int n_row)
      : n_col(n_col), n_row(n_row), data(n_col) {}

  void**& operator[](int col_idx) { return data[col_idx]; }
};

struct JoinAlgorithm {
  ExecuteResult left;
  ExecuteResult right;
  int left_col, right_col;
  const std::vector<std::tuple<size_t, DataType>>& output_attrs;

  template <typename T>
  ExecuteResult hash_join() {
    int n = left.n_row;
    int m = right.n_row;
    bool swapped = false;
    if (n < m) {
      std::swap(n, m);
      std::swap(left, right);
      std::swap(left_col, right_col);
      swapped = true;
    }
    auto translate = [&](int x) {
      if (!swapped) {
        return x;
      } else {
        if (x < right.n_col) {
          return x + left.n_col;
        } else {
          return x - right.n_col;
        }
      }
    };

    if (m <= 10) {
      int block_size =
          std::max(10000, (n + job_seeking_scheduler.num_workers() - 1) /
                              job_seeking_scheduler.num_workers());
      int num_block = (n + block_size - 1) / block_size;
      std::vector<int> block_sum(num_block << 4);
      job_seeking_scheduler.parfor(
          0, num_block,
          [&](int i) {
            int begin = i * block_size;
            int end = std::min(n, (i + 1) * block_size);
            int sum = 0;
            for (int j = begin; j < end; j++) {
              if (left[left_col][j] == nullptr) continue;
              for (int k = 0; k < m; k++) {
                if (right[right_col][k] == nullptr) continue;
                if (left[left_col][j] == right[right_col][k]) sum++;
              }
            }
            block_sum[i << 4] = sum;
          },
          1);
      for (int i = 1; i < num_block; i++) {
        // block_sum[i] += block_sum[(i - 1)<<4];
        block_sum[i] = block_sum[i - 1] + block_sum[i << 4];
      }
      int tot = block_sum[(num_block - 1)];

      ExecuteResult results(output_attrs.size(), tot);
      char* results_data = (char*)alloc::general_allocator->malloc(
          8 * output_attrs.size() * tot);
      for (int i = 0; i < output_attrs.size(); i++) {
        results[i] = (void**)(results_data + 8 * i * tot);
      }

      job_seeking_scheduler.parfor(
          0, num_block,
          [&](int i) {
            int begin = i * block_size;
            int end = std::min(n, (i + 1) * block_size);
            int pos = i == 0 ? 0 : block_sum[i - 1];
            for (int j = begin; j < end; j++) {
              if (left[left_col][j] == nullptr) continue;
              for (int k = 0; k < m; k++) {
                if (right[right_col][k] == nullptr) continue;
                if (left[left_col][j] != right[right_col][k]) continue;
                for (int ci = 0; ci < output_attrs.size(); ci++) {
                  int col_idx = std::get<0>(output_attrs[ci]);
                  col_idx = translate(col_idx);
                  if (col_idx < left.n_col) {
                    results[ci][pos] = left[col_idx][j];
                  } else {
                    col_idx -= left.n_col;
                    results[ci][pos] = right[col_idx][k];
                  }
                }
                pos++;
              }
            }
          },
          1);
      return results;

    } else {
#if defined(DEBUG_OUTPUT)
      timer t;
#endif  // DEBUG_OUTPUT

      IntegerSort* integer_sort;

      auto Build = [&]() {
        std::pair<int, int>* items =
            (std::pair<int, int>*)alloc::string_allocator->malloc(
                sizeof(std::pair<int, int>) * m);
        job_seeking_scheduler.parfor(0, m, [&](int i) {
          items[i] = {(int)((int64_t)right[right_col][i]), i};
        });
        integer_sort = new IntegerSort(items, m);
        integer_sort->Sort();
      };

      if (right.n_row >= 10000) {
        uint64_t column_hash = right.n_row;
        for (int i = 0; i < 10000; i++) {
          detail::hash_combine_impl(column_hash, (uint64_t)right[right_col][i]);
        }
        auto it = hash_table_cache.find(column_hash);
        if (it == hash_table_cache.end()) {
          Build();
          hash_table_cache[column_hash] = integer_sort;
        } else {
          integer_sort = it->second;
#if defined(DEBUG_OUTPUT)
          debug(right.n_row, integer_sort->n);
#endif  // DEBUG_OUTPUT
        }
      } else {
        Build();
      }
#if defined(DEBUG_OUTPUT)
      t.next("sort");
#endif  // DEBUG_OUTPUT

      std::pair<int, int>* tmp =
          (std::pair<int, int>*)alloc::general_allocator->malloc(
              sizeof(std::pair<int, int>) * n);
      auto& prefix_sum = *scanner_1;
      job_seeking_scheduler.parfor(0, n, [&](int i) {
        if (left[left_col][i] == nullptr) {
          prefix_sum[i] = 0;
          return;
        }
        auto res =
            integer_sort->get_interval((int)((int64_t)left[left_col][i]));
        if (res.has_value()) {
          tmp[i] = res.value();
          prefix_sum[i] = res.value().second - res.value().first;
        } else {
          prefix_sum[i] = 0;
        }
      });
      int tot = prefix_sum.scan(n);
#if defined(DEBUG_OUTPUT)
      t.next("prefix sum");
#endif  // DEBUG_OUTPUT

      ExecuteResult results(output_attrs.size(), tot);
      char* results_data = (char*)alloc::general_allocator->malloc(
          8 * output_attrs.size() * tot);
      for (int i = 0; i < output_attrs.size(); i++) {
        results[i] = (void**)(results_data + 8 * i * tot);
      }

      job_seeking_scheduler.parfor(0, n, [&](int left_idx) {
        if (left[left_col][left_idx] == nullptr) return;
        if ((left_idx < n - 1 &&
             prefix_sum[left_idx] == prefix_sum[left_idx + 1]) ||
            (left_idx == n - 1 && prefix_sum[left_idx] == tot))
          return;
        int pos = prefix_sum[left_idx];
        auto& right_indices = tmp[left_idx];
        for (int ci = 0; ci < output_attrs.size(); ci++) {
          int col_idx = std::get<0>(output_attrs[ci]);
          col_idx = translate(col_idx);
          int len = right_indices.second - right_indices.first;
          if (col_idx < left.n_col) {
            job_seeking_scheduler.parfor(0, len, [&](int j) {
              results[ci][pos + j] = left[col_idx][left_idx];
            });
          } else {
            col_idx -= left.n_col;
            job_seeking_scheduler.parfor(0, len, [&](int j) {
              int p = right_indices.first + j;
              int right_idx = integer_sort->items[p].second;
              results[ci][pos + j] = right[col_idx][right_idx];
            });
          }
        }
      });
#if defined(DEBUG_OUTPUT)
      t.next("fill results");
#endif  // DEBUG_OUTPUT
      return results;
    }
  }

  template <class T>
  ExecuteResult run() {
    if (left.n_row == 0 || right.n_row == 0) {
      return ExecuteResult(output_attrs.size(), 0);
    }
    return hash_join<T>();
  }
};

ExecuteResult execute_impl(const std::vector<PlanNode>& nodes,
                           const std::vector<ColumnarTable>& inputs,
                           int node_idx);

void print_variant(const Data& v) {
  std::visit(
      [](auto&& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, int32_t>) {
          std::cout << "int32_t: " << value << '\n';
        } else if constexpr (std::is_same_v<T, int64_t>) {
          std::cout << "int64_t: " << value << '\n';
        } else if constexpr (std::is_same_v<T, double>) {
          std::cout << "double: " << value << '\n';
        } else if constexpr (std::is_same_v<T, std::string>) {
          std::cout << "string: " << value << '\n';
        } else if constexpr (std::is_same_v<T, std::monostate>) {
          std::cout << "monostate (empty)\n";
        }
      },
      v);
}

ExecuteResult execute_scan(
    const std::vector<ColumnarTable>& inputs, const ScanNode& scan,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
#if defined(DEBUG_OUTPUT)
  timer t("scan", true);
#endif  // DEBUG_OUTPUT
  auto table_id = scan.base_table_id;
  const ColumnarTable& input = inputs[table_id];
  ExecuteResult results(output_attrs.size(), input.num_rows);
  for (int j = 0; j < output_attrs.size(); j++) {
    auto [col_idx, type] = output_attrs[j];
    // timer t;
    results[j] =
        read_column(type, input.num_rows, input.columns[col_idx].pages);
    // debug("read_column", t.stop(), (int)type);
  }
#if defined(DEBUG_OUTPUT)
  std::cout << std::string("scan[") + std::to_string(t.stop()) +
                   ", n: " + std::to_string(results.n_row) + "]\n";
#endif  // DEBUG_OUTPUT
  return results;
}

ExecuteResult execute_join(
    const std::vector<PlanNode>& nodes,
    const std::vector<ColumnarTable>& inputs, const JoinNode& join,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
  auto left_idx = join.left;
  auto right_idx = join.right;
  auto& left_node = nodes[left_idx];
  auto& right_node = nodes[right_idx];
  auto& left_types = left_node.output_attrs;
  auto& right_types = right_node.output_attrs;

  ExecuteResult left = execute_impl(nodes, inputs, left_idx);
  ExecuteResult right = execute_impl(nodes, inputs, right_idx);
  // ExecuteResult left, right;
  // parlay::parallel_do([&]() { left = execute_impl(plan, left_idx); },
  //                     [&]() { right = execute_impl(plan, right_idx); });

  timer t("join", true);
  JoinAlgorithm join_algorithm{.left = left,
                               .right = right,
                               .left_col = (int)join.left_attr,
                               .right_col = (int)join.right_attr,
                               .output_attrs = output_attrs};
  auto type = std::get<1>(left_types[join.left_attr]);
  ExecuteResult results;
  if (type == DataType::INT32) {
    results = join_algorithm.run<int32_t>();
  } else if (type == DataType::INT64) {
    results = join_algorithm.run<int64_t>();
  } else if (type == DataType::FP64) {
    results = join_algorithm.run<double>();
  } else if (type == DataType::VARCHAR) {
    results = join_algorithm.run<std::string_view>();
  } else {
    exit(1);
  }
  int n = left.n_row;
  int m = right.n_row;
#if defined(DEBUG_OUTPUT)
  std::cout << std::string("join[") + std::to_string(t.stop()) +
                   ", n: " + std::to_string(n) + ", m: " + std::to_string(m) +
                   ", type: " + std::to_string((int)type) +
                   ", build_left: " + std::to_string((int)join.build_left) +
                   "]\n";
#endif  // DEBUG_OUTPUT
  return results;
}

ExecuteResult execute_join_2(
    ExecuteResult left, ExecuteResult right, int left_attr, int right_attr,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
  // debug("execute_join_2", left.n_row, right.n_row, left_attr, right_attr,
  //       output_attrs);
  JoinAlgorithm join_algorithm{.left = left,
                               .right = right,
                               .left_col = left_attr,
                               .right_col = right_attr,
                               .output_attrs = output_attrs};
  ExecuteResult results;
  results = join_algorithm.run<int32_t>();
  return results;
}

ExecuteResult execute_impl(const std::vector<PlanNode>& nodes,
                           const std::vector<ColumnarTable>& inputs,
                           int node_idx) {
  //   debug("execute_impl", node_idx);
  auto& node = nodes[node_idx];
  return std::visit(
      [&](const auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, ScanNode>) {
          return execute_scan(inputs, value, node.output_attrs);
        } else {
          return execute_join(nodes, inputs, value, node.output_attrs);
        }
      },
      node.data);
}

template <typename Seq>
void set_bitmap(Seq& bitmap, uint16_t idx) {
  while (bitmap.size() < idx / 8 + 1) {
    bitmap.emplace_back(0);
  }
  auto byte_idx = idx / 8;
  auto bit = idx % 8;
  bitmap[byte_idx] |= (1u << bit);
}

template <typename Seq>
void unset_bitmap(Seq& bitmap, uint16_t idx) {
  while (bitmap.size() < idx / 8 + 1) {
    bitmap.emplace_back(0);
  }
  auto byte_idx = idx / 8;
  auto bit = idx % 8;
  bitmap[byte_idx] &= ~(1u << bit);
}

// return the number of pages used
int build_columnar_pages_get_page_cnt(size_t num_rows, void** result) {
  int page_cnt = 0;
  uint16_t row_cnt = 0;

  int data_size = 0, offset_size = 0;

  auto save_long_string = [&](std::string_view data) {
    size_t offset = 0;
    while (offset < data.size()) {
      page_cnt++;
      auto page_data_len = std::min(data.size() - offset, PAGE_SIZE - 4);
      offset += page_data_len;
    }
  };
  auto save_page = [&]() {
    page_cnt++;
    row_cnt = 0;
    data_size = offset_size = 0;
  };
  for (size_t row_idx = 0; row_idx < num_rows; row_idx++) {
    void* ptr = result[row_idx];
    if (ptr != nullptr) {
      std::string_view value = *(std::string_view*)ptr;
      if (value.size() > PAGE_SIZE - 7) {
        if (row_cnt > 0) {
          save_page();
        }
        save_long_string(value);
      } else {
        if (4 + (offset_size + 1) * 2 + (data_size + value.size()) +
                (row_cnt / 8 + 1) >
            PAGE_SIZE) {
          save_page();
        }
        data_size += value.size();
        offset_size++;
        ++row_cnt;
      }
    } else {
      if (4 + offset_size * 2 + data_size + (row_cnt / 8 + 1) > PAGE_SIZE) {
        save_page();
      }
      ++row_cnt;
    }
  }
  if (row_cnt != 0) {
    save_page();
  }
  return page_cnt;
}

// return the number of pages used
int build_columnar_pages_for_strings_sequential(Page** pages, size_t num_rows,
                                                void** result) {
  int page_cnt = 0;
  uint16_t row_cnt = 0;
  std::vector<char> data;
  std::vector<uint16_t> offsets;
  std::vector<uint8_t> bitmap;
  data.reserve(8192);
  offsets.reserve(4096);
  bitmap.reserve(512);
  auto save_long_string = [&](std::string_view data) {
    size_t offset = 0;
    auto first_page = true;
    while (offset < data.size()) {
      char* page = (char*)pages[page_cnt++];
      if (first_page) {
        *reinterpret_cast<uint16_t*>(page) = 0xffff;
        first_page = false;
      } else {
        *reinterpret_cast<uint16_t*>(page) = 0xfffe;
      }
      auto page_data_len = std::min(data.size() - offset, PAGE_SIZE - 4);
      *reinterpret_cast<uint16_t*>(page + 2) = page_data_len;
      memcpy(page + 4, data.data() + offset, page_data_len);
      offset += page_data_len;
    }
  };
  auto save_page = [&]() {
    char* page = (char*)pages[page_cnt++];
    *reinterpret_cast<uint16_t*>(page) = row_cnt;
    *reinterpret_cast<uint16_t*>(page + 2) =
        static_cast<uint16_t>(offsets.size());
    memcpy(page + 4, offsets.data(), offsets.size() * 2);
    memcpy(page + 4 + offsets.size() * 2, data.data(), data.size());
    memcpy(page + PAGE_SIZE - bitmap.size(), bitmap.data(), bitmap.size());
    row_cnt = 0;
    data.clear();
    offsets.clear();
    bitmap.clear();
  };
  for (size_t row_idx = 0; row_idx < num_rows; row_idx++) {
    void* ptr = result[row_idx];
    if (ptr != nullptr) {
      std::string_view value = *(std::string_view*)ptr;
      if (value.size() > PAGE_SIZE - 7) {
        if (row_cnt > 0) {
          save_page();
        }
        save_long_string(value);
      } else {
        if (4 + (offsets.size() + 1) * 2 + (data.size() + value.size()) +
                (row_cnt / 8 + 1) >
            PAGE_SIZE) {
          save_page();
        }
        set_bitmap(bitmap, row_cnt);
        data.insert(data.end(), value.begin(), value.end());
        offsets.emplace_back(data.size());
        ++row_cnt;
      }
    } else {
      if (4 + offsets.size() * 2 + data.size() + (row_cnt / 8 + 1) >
          PAGE_SIZE) {
        save_page();
      }
      unset_bitmap(bitmap, row_cnt);
      ++row_cnt;
    }
  }
  if (row_cnt != 0) {
    save_page();
  }
  return page_cnt;
}

void build_columnar_pages_for_strings(Column& column, size_t num_rows,
                                      void** result) {
  int n = num_rows;
  int block_size =
      std::max(5000, (n + job_seeking_scheduler.num_workers() - 1) /
                         job_seeking_scheduler.num_workers());
  int num_block = (n + block_size - 1) / block_size;

  std::vector<int> num_pages(num_block);
  job_seeking_scheduler.parfor(
      0, num_block,
      [&](int i) {
        int begin = i * block_size;
        int end = std::min(n, (i + 1) * block_size);
        num_pages[i] =
            build_columnar_pages_get_page_cnt(end - begin, result + begin);
      },
      1);

  std::vector<Page**> output(num_block);
  for (int i = 0; i < num_block; i++) {
    int t = num_pages[i];
    output[i] = (Page**)alloc::general_allocator->malloc(sizeof(Page*) * t);
    for (int j = 0; j < t; j++) {
      output[i][j] = new Page();
    }
  }

  job_seeking_scheduler.parfor(
      0, num_block,
      [&](int i) {
        int begin = i * block_size;
        int end = std::min(n, (i + 1) * block_size);
        num_pages[i] = build_columnar_pages_for_strings_sequential(
            output[i], end - begin, result + begin);
      },
      1);
  int page_cnt = 0;
  for (int i = 0; i < num_block; i++) {
    page_cnt += num_pages[i];
  }
  column.pages.resize(page_cnt);
  int page_idx = 0;
  for (int i = 0; i < num_block; i++) {
    for (int j = 0; j < num_pages[i]; j++) {
      column.pages[page_idx++] = output[i][j];
    }
  }
}

// sequential, but not the bottleneck
void build_columnar_pages(Column& column, size_t num_rows, void** result) {
  if (num_rows == 0) return;
  if (column.type == DataType::INT32) {
    static const size_t NUM_PER_PAGE_32 = 1984;  // 256 bits for bitmap
    static const size_t NUM_PER_PAGE_64 = 1007;  // 128 bits for bitmap
    size_t num_pages =
        num_rows / NUM_PER_PAGE_32 + (num_rows % NUM_PER_PAGE_32 ? 1 : 0);
    column.pages.resize(num_pages);
    std::vector<uint8_t> bitmap;
    for (size_t page_id = 0; page_id < num_pages; page_id++) {
      // column.pages[page_id] = parlay::type_allocator<Page>::create();
      column.pages[page_id] = new Page();
      char* page = (char*)column.pages[page_id];
      size_t l = page_id * NUM_PER_PAGE_32;
      size_t r = std::min(l + NUM_PER_PAGE_32, num_rows);
      uint16_t nr = r - l;
      uint16_t nv = 0;
      int row_cnt = 0;
      for (size_t i = l; i < r; i++) {
        if (result[i] != nullptr) {
          *reinterpret_cast<int32_t*>(page + 4 + nv * 4) =
              GetData<int32_t>(result[i]);
          set_bitmap(bitmap, row_cnt);
          nv++;
        } else {
          unset_bitmap(bitmap, row_cnt);
        }
        row_cnt++;
      }
      *reinterpret_cast<uint16_t*>(page) = nr;
      *reinterpret_cast<uint16_t*>(page + 2) = nv;
      memcpy(page + PAGE_SIZE - bitmap.size(), bitmap.data(), bitmap.size());
      bitmap.clear();
    }
  } else if (column.type == DataType::INT64 || column.type == DataType::FP64) {
    debug("no implementation for INT64/FP64");
    exit(1);
  } else {
    build_columnar_pages_for_strings(column, num_rows, result);
  }
}

ColumnarTable execute(const Plan& plan0, [[maybe_unused]] void* context) {
  std::vector<PlanNode> nodes = plan0.nodes;
  const std::vector<ColumnarTable>& inputs = plan0.inputs;
  int root = plan0.root;
  int tot = nodes.size();
  int num = inputs.size();

  // std::cout << "\n";
  // for (int i = 0; i < tot; i++) {
  //   auto& node = nodes[i];
  //   std::vector<int> out;
  //   if (std::holds_alternative<ScanNode>(node.data)) {
  //     auto& scan_node = std::get<ScanNode>(node.data);
  //     for (auto [col_idx, type] : node.output_attrs) {
  //       out.push_back(col_idx);
  //     }
  //     debug("scan", i, scan_node.base_table_id, out);
  //   } else {
  //     auto& join_node = std::get<JoinNode>(node.data);
  //     for (auto [col_idx, type] : node.output_attrs) {
  //       out.push_back(col_idx);
  //     }
  //     debug("join", i, join_node.left, join_node.right, join_node.left_attr,
  //           join_node.right_attr, out);
  //   }
  // }

  std::vector<int> input_to_node_id(num, -1);
  for (int i = 0; i < tot; i++) {
    auto& node = nodes[i];
    if (std::holds_alternative<ScanNode>(node.data)) {
      auto& scan_node = std::get<ScanNode>(node.data);
      input_to_node_id[scan_node.base_table_id] = i;
    }
  }

  std::vector<int> que;
  que.push_back(root);
  for (int i = 0; i < (int)que.size(); i++) {
    int node_idx = que[i];
    auto& node = nodes[node_idx];
    if (std::holds_alternative<ScanNode>(node.data)) {
    } else {
      auto& join_node = std::get<JoinNode>(node.data);
      que.push_back(join_node.left);
      que.push_back(join_node.right);
    }
  }
  // debug(que);
  std::vector<std::vector<std::pair<int, std::pair<int, DataType>>>> attrs(tot);
  for (int i = tot - 1; i >= 0; i--) {
    int node_idx = que[i];
    auto& node = nodes[node_idx];
    if (std::holds_alternative<ScanNode>(node.data)) {
      auto& scan_node = std::get<ScanNode>(node.data);
      for (int j = 0; j < node.output_attrs.size(); j++) {
        auto [col_idx, type] = node.output_attrs[j];
        attrs[node_idx].push_back(
            {(int)scan_node.base_table_id, {(int)col_idx, type}});
      }
    } else {
      auto& join_node = std::get<JoinNode>(node.data);
      int l = join_node.left, r = join_node.right;
      for (int j = 0; j < node.output_attrs.size(); j++) {
        auto [col_idx, type] = node.output_attrs[j];
        if (col_idx < attrs[l].size()) {
          attrs[node_idx].push_back(attrs[l][col_idx]);
        } else {
          attrs[node_idx].push_back(attrs[r][col_idx - attrs[l].size()]);
        }
      }
    }
  }
  // for (int i = 0; i < tot; i++) {
  //   debug(i, attrs[i]);
  // }

  std::vector adj(
      tot, std::vector(
               tot, std::optional<std::pair<int, std::pair<int, DataType>>>()));
  for (int i = 0; i < tot; i++) {
    for (int j = 0; j < tot; j++) {
      adj[i][j] = std::nullopt;
    }
  }
  for (int i = 0; i < tot; i++) {
    auto& node = nodes[i];
    if (std::holds_alternative<JoinNode>(node.data)) {
      auto& join_node = std::get<JoinNode>(node.data);
      int l = join_node.left, r = join_node.right;
      int a = input_to_node_id[attrs[l][join_node.left_attr].first];
      int b = input_to_node_id[attrs[r][join_node.right_attr].first];
      adj[a][b] = attrs[l][join_node.left_attr];
      adj[b][a] = attrs[r][join_node.right_attr];
    }
  }

  std::map<std::pair<int, std::pair<int, DataType>>, int> final_attr_to_id;
  std::set<std::pair<int, std::pair<int, DataType>>> final_output_attrs;
  for (int i = 0; i < (int)attrs[root].size(); i++) {
    final_attr_to_id[attrs[root][i]] = i;
    final_output_attrs.insert(attrs[root][i]);
  }
  // debug(final_output_attrs);

  for (int i = 0; i < tot; i++) {
    if (std::holds_alternative<JoinNode>(nodes[i].data)) {
      attrs[i].clear();
    } else {
      std::sort(attrs[i].begin(), attrs[i].end());
    }
  }
  // debug("after clear");
  // for (int i = 0; i < tot; i++) {
  //   debug(i, attrs[i]);
  // }
  // for (int i = 0; i < tot; i++) {
  //   debug(i, adj[i]);
  // }

  std::vector<std::optional<ExecuteResult>> results(tot, std::nullopt);
  for (int i = 0; i < tot; i++) {
    auto& node = nodes[i];
    if (std::holds_alternative<ScanNode>(node.data)) {
      auto& scan_node = std::get<ScanNode>(node.data);
      std::vector<std::tuple<size_t, DataType>> output_attrs;
      for (int j = 0; j < (int)attrs[i].size(); j++) {
        output_attrs.push_back(
            {attrs[i][j].second.first, attrs[i][j].second.second});
      }
      results[i] = execute_scan(inputs, scan_node, output_attrs);
    }
  }

  auto GetId = [&](int x, std::pair<int, std::pair<int, DataType>> t) {
    for (int i = 0; i < (int)attrs[x].size(); i++) {
      if (attrs[x][i] == t) return i;
    }
    debug("GetId fail", x, t);
    exit(1);
  };

  int cur = 0;

  auto Work = [&](int l, int r) {
    while (!attrs[cur].empty()) cur++;
    // lndebug("work", l, r, cur);

    std::set<std::pair<int, std::pair<int, DataType>>> needl, needr;
    for (int i = 0; i < tot; i++) {
      if (i != r && adj[l][i].has_value()) needl.insert(adj[l][i].value());
      if (i != l && adj[r][i].has_value()) needr.insert(adj[r][i].value());
    }
    // debug(needl, needr);

    ExecuteResult left = results[l].value();
    ExecuteResult right = results[r].value();
    auto left_col = GetId(l, adj[l][r].value());
    auto right_col = GetId(r, adj[r][l].value());
    // debug(left_col, right_col);
    std::vector<std::pair<std::pair<int, std::pair<int, DataType>>, int>>
        output_attrs_0;
    for (int i = 0; i < (int)attrs[l].size(); i++) {
      if (needl.find(attrs[l][i]) != needl.end() ||
          final_output_attrs.find(attrs[l][i]) != final_output_attrs.end()) {
        output_attrs_0.push_back({attrs[l][i], i});
      }
    }
    for (int i = 0; i < (int)attrs[r].size(); i++) {
      if (needr.find(attrs[r][i]) != needr.end() ||
          final_output_attrs.find(attrs[r][i]) != final_output_attrs.end()) {
        output_attrs_0.push_back({attrs[r][i], i + attrs[l].size()});
      }
    }
    std::sort(output_attrs_0.begin(), output_attrs_0.end());
    // debug(output_attrs_0);

    std::vector<std::tuple<size_t, DataType>> output_attrs;
    for (int i = 0; i < (int)output_attrs_0.size(); i++) {
      auto [attr, idx] = output_attrs_0[i];
      attrs[cur].push_back(attr);
      output_attrs.push_back({idx, attr.second.second});
    }
    // debug(output_attrs);

    results[cur] =
        execute_join_2(left, right, left_col, right_col, output_attrs);

    for (int i = 0; i < tot; i++) {
      if (i != r && adj[l][i].has_value()) {
        adj[cur][i] = adj[l][i];
        adj[i][cur] = adj[i][l];
        adj[l][i] = adj[i][l] = std::nullopt;
      }
      if (i != l && adj[r][i].has_value()) {
        adj[cur][i] = adj[r][i];
        adj[i][cur] = adj[i][r];
        adj[r][i] = adj[i][r] = std::nullopt;
      }
    }

    results[l] = std::nullopt;
    results[r] = std::nullopt;
    // debug("Work End");
  };

  for (int round = 0; round < num - 1; round++) {
    int best_i = -1, best_j = -1;
    uint64_t best_cost = std::numeric_limits<uint64_t>::max();
    for (int i = 0; i < tot; i++) {
      for (int j = 0; j < tot; j++) {
        if (i == j || !results[i].has_value() || !results[j].has_value())
          continue;
        if (adj[i][j].has_value()) {
          uint64_t cost =
              (uint64_t)results[i].value().n_row * results[j].value().n_row;
          if (cost < best_cost) {
            best_cost = cost;
            best_i = i;
            best_j = j;
          }
        }
      }
    }
    // for (int i = 0; i < tot; i++) {
    //   if (!results[i].has_value()) continue;
    //   int j = -1;
    //   for (int k = 0; k < tot; k++) {
    //     if (i == k) continue;
    //     if (adj[i][k].has_value()) {
    //       j = k;
    //       break;
    //     }
    //   }
    //   if (j == -1 || !results[j].has_value()) {
    //     debug("bad", i, j);
    //     exit(1);
    //   }
    //   Work(i, j);
    //   break;
    // }

    Work(best_i, best_j);

    // for (int i = 0; i < tot; i++) {
    //   debug(i, attrs[i]);
    // }
    // for (int i = 0; i < tot; i++) {
    //   debug(i, adj[i]);
    // }
  }

  ExecuteResult tmp = results[cur].value();
  for (auto& [k, v] : final_attr_to_id) {
    for (int i = 0; i < (int)attrs[cur].size(); i++) {
      if (attrs[cur][i] == k) {
        tmp[v] = results[cur].value()[i];
      }
    }
  }

#if defined(DEBUG_OUTPUT)
  std::cout << "\n";
#endif  // DEBUG_OUTPUT
  // ExecuteResult tmp = execute_impl(nodes, inputs, root);
  timer t;
  int m = tmp.n_col;
  int n = tmp.n_row;
#if defined(DEBUG_OUTPUT)
  debug("final result", n, m);
#endif  // DEBUG_OUTPUT
  ColumnarTable columnar_table;
  columnar_table.num_rows = n;
  for (auto [_, type] : nodes[root].output_attrs) {
    columnar_table.columns.emplace_back(type);
  }
  for (int j = 0; j < m; j++) {
    build_columnar_pages(columnar_table.columns[j], n, tmp[j]);
  }
#if defined(DEBUG_OUTPUT)
  auto ret_types = nodes[root].output_attrs |
                   ranges::views::transform(
                       [](const auto& v) { return (int)std::get<1>(v); }) |
                   ranges::to<std::vector<int>>();
  debug(ret_types);
  debug("build_columnar_pages", t.stop());
#endif  // DEBUG_OUTPUT

  alloc::general_allocator->clear();  // clear buffer for general allocator

  return columnar_table;
}

void* build_context() {
  lndebug("build_context start");
  job_seeking_context = new JobSeekingContext();
  debug(job_seeking_scheduler.num_workers());
  debug(std::thread::hardware_concurrency());
  debug("build_context done");
  return job_seeking_context;
}

void destroy_context([[maybe_unused]] void* context) {
  debug("destroy_context start");
  delete job_seeking_context;
  debug("destroy_context done");
}

}  // namespace job_seeking

void test_integer_sort() { exit(0); }

// ==================================================================================================================================

namespace Contest {
using ExecuteResult = std::vector<std::vector<Data>>;

ExecuteResult execute_impl(const Plan& plan, size_t node_idx);

struct JoinAlgorithm {
  bool build_left;
  ExecuteResult& left;
  ExecuteResult& right;
  ExecuteResult& results;
  size_t left_col, right_col;
  const std::vector<std::tuple<size_t, DataType>>& output_attrs;

  template <class T>
  auto run() {
    namespace views = ranges::views;
    std::unordered_map<T, std::vector<size_t>> hash_table;
    if (build_left) {
      for (auto&& [idx, record] : left | views::enumerate) {
        std::visit(
            [&hash_table, idx = idx](const auto& key) {
              using Tk = std::decay_t<decltype(key)>;
              if constexpr (std::is_same_v<Tk, T>) {
                if (auto itr = hash_table.find(key); itr == hash_table.end()) {
                  hash_table.emplace(key, std::vector<size_t>(1, idx));
                } else {
                  itr->second.push_back(idx);
                }
              } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                throw std::runtime_error("wrong type of field");
              }
            },
            record[left_col]);
      }
      for (auto& right_record : right) {
        std::visit(
            [&](const auto& key) {
              using Tk = std::decay_t<decltype(key)>;
              if constexpr (std::is_same_v<Tk, T>) {
                if (auto itr = hash_table.find(key); itr != hash_table.end()) {
                  for (auto left_idx : itr->second) {
                    auto& left_record = left[left_idx];
                    std::vector<Data> new_record;
                    new_record.reserve(output_attrs.size());
                    for (auto [col_idx, _] : output_attrs) {
                      if (col_idx < left_record.size()) {
                        new_record.emplace_back(left_record[col_idx]);
                      } else {
                        new_record.emplace_back(
                            right_record[col_idx - left_record.size()]);
                      }
                    }
                    results.emplace_back(std::move(new_record));
                  }
                }
              } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                throw std::runtime_error("wrong type of field");
              }
            },
            right_record[right_col]);
      }
    } else {
      for (auto&& [idx, record] : right | views::enumerate) {
        std::visit(
            [&hash_table, idx = idx](const auto& key) {
              using Tk = std::decay_t<decltype(key)>;
              if constexpr (std::is_same_v<Tk, T>) {
                if (auto itr = hash_table.find(key); itr == hash_table.end()) {
                  hash_table.emplace(key, std::vector<size_t>(1, idx));
                } else {
                  itr->second.push_back(idx);
                }
              } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                throw std::runtime_error("wrong type of field");
              }
            },
            record[right_col]);
      }
      for (auto& left_record : left) {
        std::visit(
            [&](const auto& key) {
              using Tk = std::decay_t<decltype(key)>;
              if constexpr (std::is_same_v<Tk, T>) {
                if (auto itr = hash_table.find(key); itr != hash_table.end()) {
                  for (auto right_idx : itr->second) {
                    auto& right_record = right[right_idx];
                    std::vector<Data> new_record;
                    new_record.reserve(output_attrs.size());
                    for (auto [col_idx, _] : output_attrs) {
                      if (col_idx < left_record.size()) {
                        new_record.emplace_back(left_record[col_idx]);
                      } else {
                        new_record.emplace_back(
                            right_record[col_idx - left_record.size()]);
                      }
                    }
                    results.emplace_back(std::move(new_record));
                  }
                }
              } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                throw std::runtime_error("wrong type of field");
              }
            },
            left_record[left_col]);
      }
    }
  }
};

ExecuteResult execute_hash_join(
    const Plan& plan, const JoinNode& join,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
  auto left_idx = join.left;
  auto right_idx = join.right;
  auto& left_node = plan.nodes[left_idx];
  auto& right_node = plan.nodes[right_idx];
  auto& left_types = left_node.output_attrs;
  auto& right_types = right_node.output_attrs;
  auto left = execute_impl(plan, left_idx);
  auto right = execute_impl(plan, right_idx);
  std::vector<std::vector<Data>> results;

  JoinAlgorithm join_algorithm{.build_left = join.build_left,
                               .left = left,
                               .right = right,
                               .results = results,
                               .left_col = join.left_attr,
                               .right_col = join.right_attr,
                               .output_attrs = output_attrs};
  if (join.build_left) {
    switch (std::get<1>(left_types[join.left_attr])) {
      case DataType::INT32:
        join_algorithm.run<int32_t>();
        break;
      case DataType::INT64:
        join_algorithm.run<int64_t>();
        break;
      case DataType::FP64:
        join_algorithm.run<double>();
        break;
      case DataType::VARCHAR:
        join_algorithm.run<std::string>();
        break;
    }
  } else {
    switch (std::get<1>(right_types[join.right_attr])) {
      case DataType::INT32:
        join_algorithm.run<int32_t>();
        break;
      case DataType::INT64:
        join_algorithm.run<int64_t>();
        break;
      case DataType::FP64:
        join_algorithm.run<double>();
        break;
      case DataType::VARCHAR:
        join_algorithm.run<std::string>();
        break;
    }
  }

  return results;
}

ExecuteResult execute_scan(
    const Plan& plan, const ScanNode& scan,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
  auto table_id = scan.base_table_id;
  auto& input = plan.inputs[table_id];
  auto table = Table::from_columnar(input);
  std::vector<std::vector<Data>> results;
  for (auto& record : table.table()) {
    std::vector<Data> new_record;
    new_record.reserve(output_attrs.size());
    for (auto [col_idx, _] : output_attrs) {
      new_record.emplace_back(record[col_idx]);
    }
    results.emplace_back(std::move(new_record));
  }
  return results;
}

ExecuteResult execute_impl(const Plan& plan, size_t node_idx) {
  auto& node = plan.nodes[node_idx];
  return std::visit(
      [&](const auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, JoinNode>) {
          return execute_hash_join(plan, value, node.output_attrs);
        } else {
          return execute_scan(plan, value, node.output_attrs);
        }
      },
      node.data);
}

ColumnarTable execute(const Plan& plan, [[maybe_unused]] void* context) {
#ifdef JOB_SEEKING
  return job_seeking::execute(plan, context);
#else
  namespace views = ranges::views;
  auto ret = execute_impl(plan, plan.root);
  auto ret_types =
      plan.nodes[plan.root].output_attrs |
      views::transform([](const auto& v) { return std::get<1>(v); }) |
      ranges::to<std::vector<DataType>>();
  Table table{std::move(ret), std::move(ret_types)};
  return table.to_columnar();
#endif  // JOB_SEEKING
}

void* build_context() {
#ifdef JOB_SEEKING
  return job_seeking::build_context();
#else
  return nullptr;
#endif  // JOB_SEEKING
}

void destroy_context([[maybe_unused]] void* context) {
#ifdef JOB_SEEKING
  job_seeking::destroy_context(context);
#else
  return;
#endif  // JOB_SEEKING
}

}  // namespace Contest
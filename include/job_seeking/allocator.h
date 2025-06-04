#ifndef JOBSEEKING_ALLOCATOR_H_
#define JOBSEEKING_ALLOCATOR_H_

#include <iostream>
#include <vector>

#include "debug.h"
#include "my_scheduler.h"

#if defined(SPC__LEVEL1_ICACHE_LINESIZE)
const int align = SPC__LEVEL1_ICACHE_LINESIZE;
#else
const int align = 64;
#endif

namespace job_seeking {

namespace alloc {

template <bool clean>
struct my_allocator {
  char* buffer;
  std::vector<char*> all_buffers;
  size_t size;
  size_t used_size;
  size_t max_history_size;
  int alloc_cnt = 0;

  my_allocator(size_t size) : size(size) { alloc_buffer(); }

  void alloc_buffer() {
    debug("alloc", this->size);
    alloc_cnt++;
    buffer = new char[size];
    all_buffers.push_back(buffer);
    used_size = 0;
    max_history_size = 0;
    if constexpr (clean) {
      void** tmp = (void**)buffer;
      job_seeking_scheduler.parfor(0, size / 8,
                                   [&](size_t i) { tmp[i] = nullptr; });
    }
  }

  void* malloc(size_t size) {
    used_size = (used_size + align - 1) / align * align;
    if (used_size + size >= this->size) {
      this->size = std::max(this->size * 2, this->size + size);
      alloc_buffer();
    }
    char* ret = buffer + used_size;
    used_size += size;
    return ret;
  }

  void clear() {
    max_history_size = std::max(max_history_size, used_size);
    used_size = 0;
  }
};

my_allocator<true>* data_allocator;
my_allocator<false>* string_allocator;
my_allocator<false>* general_allocator;

template <typename T>
struct cpp_allocator {
  using value_type = T;

  cpp_allocator() {}

  template <typename U>
  cpp_allocator(const cpp_allocator<U>&) {}

  T* allocate(std::size_t n) {
    void* ptr = general_allocator->malloc(n * sizeof(T));
    return static_cast<T*>(ptr);
  }

  void deallocate(T* ptr, std::size_t) {
    // no free
  }
};
template <typename T, typename U>
bool operator==(const cpp_allocator<T>&, const cpp_allocator<U>&) {
  return true;
}

template <typename T, typename U>
bool operator!=(const cpp_allocator<T>&, const cpp_allocator<U>&) {
  return false;
}

}  // namespace alloc

}  // namespace job_seeking

#endif  // JOBSEEKING_ALLOCATOR_H_

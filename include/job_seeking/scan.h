#ifndef JOB_SEEKING_SCAN_H_
#define JOB_SEEKING_SCAN_H_

// #include "parallel.h"
#include "my_scheduler.h"

namespace job_seeking {

const int scan_block_size = 10000;
const int a_size = 40000000;
const int b_size = 10000;

template <typename T>
T my_scan(int n, T* a, T* b) {
  if (n <= scan_block_size) {
    T sum = 0;
    for (int i = 0; i < n; i++) {
      T t = sum + a[i];
      a[i] = sum;
      sum = t;
    }
    return sum;
  }
  int bs =
      std::max(scan_block_size, (n + job_seeking_scheduler.num_workers() - 1) /
                                    job_seeking_scheduler.num_workers());
  int num_blocks = (n + bs - 1) / bs;
  job_seeking_scheduler.parfor(
      0, num_blocks,
      [&](int i) {
        int start = i * bs;
        int end = std::min(start + bs, n);
        int sum = 0;
        for (int j = start; j < end; j++) {
          sum += a[j];
        }
        b[i<<4] = sum;
      },
      1);
  for (int i = 1; i < num_blocks; i++) {
    // b[i] += b[i - 1];
    b[i] = b[i-1]+b[i<<4];
  }
  job_seeking_scheduler.parfor(
      0, num_blocks,
      [&](int i) {
        int start = i * bs;
        int end = std::min(start + bs, n);
        int sum = i == 0 ? 0 : b[i - 1];
        for (int j = start; j < end; j++) {
          int t = sum + a[j];
          a[j] = sum;
          sum = t;
        }
      },
      1);
  return b[num_blocks - 1];
}

struct scanner {
  std::vector<int> a, b;

  scanner() : a(a_size), b(b_size) {}

  int& operator[](int i) { return a[i]; }

  int scan(int n) { return my_scan(n, a.data(), b.data()); }
};

scanner* scanner_1;
scanner* scanner_2;

}  // namespace job_seeking

#endif  // JOB_SEEKING_SCAN_H_
